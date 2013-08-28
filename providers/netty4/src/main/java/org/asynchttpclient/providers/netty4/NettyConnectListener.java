/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
package org.asynchttpclient.providers.netty4;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.HostnameVerifier;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.ProxyServer;
import org.asynchttpclient.Request;
import org.asynchttpclient.util.ProxyUtils;


/**
 * Non Blocking connect.
 */
// FIXME Netty 3: NettyConnectListener don't need to be passed the request as the future has it too
final class NettyConnectListener<T> implements ChannelFutureListener {
    private final static Logger logger = LoggerFactory.getLogger(NettyConnectListener.class);
    private final AsyncHttpClientConfig config;
    private final NettyResponseFuture<T> future;
    private final AtomicBoolean handshakeDone = new AtomicBoolean(false);

    private NettyConnectListener(AsyncHttpClientConfig config,
                                 NettyResponseFuture<T> future) {
        this.config = config;
        this.future = future;
    }

    public NettyResponseFuture<T> future() {
        return future;
    }

    private void onFutureSuccess(final Channel channel) throws Exception {
        channel.pipeline().context(NettyAsyncHttpProvider.class).attr(NettyAsyncHttpProvider.DEFAULT_ATTRIBUTE).set(future);
        SslHandler sslHandler = (SslHandler) channel.pipeline().get(NettyAsyncHttpProvider.SSL_HANDLER);
        
        if (sslHandler != null && !handshakeDone.getAndSet(true)) {
            sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                public void operationComplete(Future<Channel> f) throws Exception {
                    if (f.isSuccess()) {
                        onFutureSuccess(channel);
                    } else {
                        onFutureFailure(channel, f.cause());
                    }
                }
            });
            return;
        }

        HostnameVerifier v = config.getHostnameVerifier();
        if (sslHandler != null) {
            if (!v.verify(future.getURI().getHost(), sslHandler.engine().getSession())) {
                ConnectException exception = new ConnectException("HostnameVerifier exception.");
                future.abort(exception);
                throw exception;
            }
        }

        future.provider().writeRequest(channel, config, future);
    }
    
    private void onFutureFailure(Channel channel, Throwable cause) throws Exception {

        logger.debug("Trying to recover a dead cached channel {} with a retry value of {} ", channel, future.canRetry());
        if (future.canRetry() && cause != null && (NettyAsyncHttpProvider.abortOnDisconnectException(cause)
                || cause instanceof ClosedChannelException
                || future.getState() != NettyResponseFuture.STATE.NEW)) {

            logger.debug("Retrying {} ", future.getNettyRequest());
            if (future.provider().remotelyClosed(channel, future)) {
                return;
            }
        }

        logger.debug("Failed to recover from exception: {} with channel {}", cause, channel);

        boolean printCause = cause != null && cause.getMessage() != null;
        ConnectException e = new ConnectException(printCause ? cause.getMessage() + " to " + future.getURI().toString() : future.getURI().toString());
        if (cause != null) {
            e.initCause(cause);
        }
        future.abort(e);
    }
    
    public final void operationComplete(ChannelFuture f) throws Exception {
        if (f.isSuccess()) {
            onFutureSuccess(f.channel());
        } else {
            onFutureFailure(f.channel(), f.cause());
        }
    }

    public static class Builder<T> {
        private final AsyncHttpClientConfig config;

        private final Request request;
        private final AsyncHandler<T> asyncHandler;
        private NettyResponseFuture<T> future;
        private final NettyAsyncHttpProvider provider;

        // FIXME Netty3 useless constructor
        public Builder(AsyncHttpClientConfig config, Request request, AsyncHandler<T> asyncHandler,
                       NettyResponseFuture<T> future, NettyAsyncHttpProvider provider) {

            this.config = config;
            this.request = request;
            this.asyncHandler = asyncHandler;
            this.future = future;
            this.provider = provider;
        }

        public NettyConnectListener<T> build(final URI uri) throws IOException {
            ProxyServer proxyServer = ProxyUtils.getProxyServer(config, request);
            HttpRequest nettyRequest = NettyAsyncHttpProvider.buildRequest(config, request, uri, true, proxyServer);
            if (future == null) {
                future = NettyAsyncHttpProvider.newFuture(uri, request, asyncHandler, nettyRequest, config, provider, proxyServer);
            } else {
                future.setNettyRequest(nettyRequest);
                future.setRequest(request);
            }
            return new NettyConnectListener<T>(config, future);
        }
    }
}
