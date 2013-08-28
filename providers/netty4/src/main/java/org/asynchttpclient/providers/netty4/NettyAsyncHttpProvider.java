/*
 * Copyright 2010-2013 Ning, Inc.
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
 */
package org.asynchttpclient.providers.netty4;

import static org.asynchttpclient.util.AsyncHttpProviderUtils.DEFAULT_CHARSET;
import static org.asynchttpclient.util.DateUtil.millisTime;
import static org.asynchttpclient.util.MiscUtil.isNonEmpty;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FileRegion;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocket08FrameDecoder;
import io.netty.handler.codec.http.websocketx.WebSocket08FrameEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.stream.ChunkedStream;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.AttributeKey;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHandler.STATE;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.AsyncHttpProvider;
import org.asynchttpclient.Body;
import org.asynchttpclient.BodyGenerator;
import org.asynchttpclient.ConnectionPoolKeyStrategy;
import org.asynchttpclient.ConnectionsPool;
import org.asynchttpclient.Cookie;
import org.asynchttpclient.FluentCaseInsensitiveStringsMap;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.MaxRedirectException;
import org.asynchttpclient.ProgressAsyncHandler;
import org.asynchttpclient.ProxyServer;
import org.asynchttpclient.RandomAccessBody;
import org.asynchttpclient.Realm;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.filter.FilterContext;
import org.asynchttpclient.filter.FilterException;
import org.asynchttpclient.filter.IOExceptionFilter;
import org.asynchttpclient.filter.ResponseFilter;
import org.asynchttpclient.generators.InputStreamBodyGenerator;
import org.asynchttpclient.listener.TransferCompletionHandler;
import org.asynchttpclient.multipart.MultipartBody;
import org.asynchttpclient.multipart.MultipartRequestEntity;
import org.asynchttpclient.ntlm.NTLMEngine;
import org.asynchttpclient.ntlm.NTLMEngineException;
import org.asynchttpclient.org.jboss.netty.handler.codec.http.CookieDecoder;
import org.asynchttpclient.org.jboss.netty.handler.codec.http.CookieEncoder;
import org.asynchttpclient.providers.netty4.FeedableBodyGenerator.FeedListener;
import org.asynchttpclient.providers.netty4.spnego.SpnegoEngine;
import org.asynchttpclient.providers.netty4.util.CleanupChannelGroup;
import org.asynchttpclient.util.AsyncHttpProviderUtils;
import org.asynchttpclient.util.AuthenticatorUtils;
import org.asynchttpclient.util.ProxyUtils;
import org.asynchttpclient.util.SslUtils;
import org.asynchttpclient.util.UTF8UrlEncoder;
import org.asynchttpclient.websocket.WebSocketUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class NettyAsyncHttpProvider extends ChannelInboundHandlerAdapter implements AsyncHttpProvider {
    private final static String HTTP_HANDLER = "httpHandler";
    protected final static String SSL_HANDLER = "sslHandler";
    private final static String HTTPS = "https";
    private final static String HTTP = "http";
    private static final String WEBSOCKET = "ws";
    private static final String WEBSOCKET_SSL = "wss";

    private final static Logger log = LoggerFactory.getLogger(NettyAsyncHttpProvider.class);
    private final static Charset UTF8 = Charset.forName("UTF-8");
    public final static AttributeKey<Object> DEFAULT_ATTRIBUTE = new AttributeKey<Object>("default");

    private final Bootstrap plainBootstrap;
    private final Bootstrap secureBootstrap;
    private final Bootstrap webSocketBootstrap;
    private final Bootstrap secureWebSocketBootstrap;
    private EventLoopGroup eventLoop;
    private final static int MAX_BUFFERED_BYTES = 8192;
    private final AsyncHttpClientConfig config;
    private final AtomicBoolean isClose = new AtomicBoolean(false);
    private final Class<? extends SocketChannel> socketChannelFactory;
    private final boolean allowReleaseSocketChannelFactory;

    private final ChannelGroup openChannels = new
            CleanupChannelGroup("asyncHttpClient") {
                @Override
                public boolean remove(Object o) {
                    boolean removed = super.remove(o);
                    if (removed && trackConnections) {
                        freeConnections.release();
                    }
                    return removed;
                }
            };
    private final ConnectionsPool<String, Channel> connectionsPool;
    private Semaphore freeConnections = null;
    private final NettyAsyncHttpProviderConfig asyncHttpProviderConfig;
    private boolean executeConnectAsync = true;
    public static final ThreadLocal<Boolean> IN_IO_THREAD = new ThreadLocalBoolean();
    private final boolean trackConnections;
    private final boolean useRawUrl;
    private final static NTLMEngine ntlmEngine = new NTLMEngine();
    private static SpnegoEngine spnegoEngine = null;
    private final Protocol httpProtocol = new HttpProtocol();
    private final Protocol webSocketProtocol = new WebSocketProtocol();

	private static boolean isNTLM(List<String> auth) {
		return isNonEmpty(auth) && auth.get(0).startsWith("NTLM");
	}

    public NettyAsyncHttpProvider(AsyncHttpClientConfig config) {

        if (config.getAsyncHttpProviderConfig() instanceof NettyAsyncHttpProviderConfig) {
            asyncHttpProviderConfig = NettyAsyncHttpProviderConfig.class.cast(config.getAsyncHttpProviderConfig());
        } else {
            asyncHttpProviderConfig = new NettyAsyncHttpProviderConfig();
        }

        if (asyncHttpProviderConfig.isUseBlockingIO()) {
            socketChannelFactory = OioSocketChannel.class;
            this.allowReleaseSocketChannelFactory = true;
        } else {
            // check if external NioClientSocketChannelFactory is defined
            Class<? extends SocketChannel> scf = asyncHttpProviderConfig.getSocketChannel();
            if (scf != null) {
                this.socketChannelFactory = scf;

                // cannot allow releasing shared channel factory
                this.allowReleaseSocketChannelFactory = false;
            } else {
                socketChannelFactory = NioSocketChannel.class;
                eventLoop = asyncHttpProviderConfig.getEventLoopGroup();
                if (eventLoop == null) {
                    if (socketChannelFactory == OioSocketChannel.class) {
                        eventLoop = new OioEventLoopGroup();
                    } else if (socketChannelFactory == NioSocketChannel.class) {
                        eventLoop = new NioEventLoopGroup();
                    } else {
                        throw new IllegalArgumentException("No set event loop compatbile with socket channel " + scf);
                    }
                }
            	int numWorkers = config.getIoThreadMultiplier() * Runtime.getRuntime().availableProcessors();
            	log.debug("Number of application's worker threads is {}", numWorkers);
            	this.allowReleaseSocketChannelFactory = true;
            }
        }
        plainBootstrap = new Bootstrap().channel(socketChannelFactory).group(eventLoop);
        secureBootstrap = new Bootstrap().channel(socketChannelFactory).group(eventLoop);
        webSocketBootstrap = new Bootstrap().channel(socketChannelFactory).group(eventLoop);
        secureWebSocketBootstrap = new Bootstrap().channel(socketChannelFactory).group(eventLoop);
        this.config = config;
        configureNetty();

        // This is dangerous as we can't catch a wrong typed ConnectionsPool
        ConnectionsPool<String, Channel> cp = (ConnectionsPool<String, Channel>) config.getConnectionsPool();
        if (cp == null && config.getAllowPoolingConnection()) {
            cp = new NettyConnectionsPool(this);
        } else if (cp == null) {
            cp = new NonConnectionsPool();
        }
        this.connectionsPool = cp;

        if (config.getMaxTotalConnections() != -1) {
            trackConnections = true;
            freeConnections = new Semaphore(config.getMaxTotalConnections());
        } else {
            trackConnections = false;
        }

        useRawUrl = config.isUseRawUrl();
    }

    @Override
    public String toString() {
        return String.format("NettyAsyncHttpProvider4:\n\t- maxConnections: %d\n\t- openChannels: %s\n\t- connectionPools: %s",
                config.getMaxTotalConnections() - freeConnections.availablePermits(),
                openChannels.toString(),
                connectionsPool.toString());
    }

    void configureNetty() {
        Map<String, ChannelOption<Object>> optionMap = new HashMap<String, ChannelOption<Object>>();
        for (Field field : ChannelOption.class.getDeclaredFields()) {
            if (field.getType().isAssignableFrom(ChannelOption.class)) {
                field.setAccessible(true);
                try {
                    optionMap.put(field.getName(), (ChannelOption<Object>) field.get(null));
                } catch (IllegalAccessException ex) {
                    throw new Error(ex);
                }
            }
        }

        if (asyncHttpProviderConfig != null) {
            for (Entry<String, Object> entry : asyncHttpProviderConfig.propertiesSet()) {
                ChannelOption<Object> key = optionMap.get(entry.getKey());
                if (key != null) {
                    Object value = entry.getValue();
                    plainBootstrap.option(key, value);
                    webSocketBootstrap.option(key, value);
                    secureBootstrap.option(key, value);
                    secureWebSocketBootstrap.option(key, value);
                } else {
                    throw new IllegalArgumentException("Unknown config property " + entry.getKey());
                }
            }
        }

        plainBootstrap.handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
                initPlainChannel(ch);
            }
        });
        // FIXME
        // DefaultChannelFuture.setUseDeadLockChecker(false);

        if (asyncHttpProviderConfig != null) {
            executeConnectAsync = asyncHttpProviderConfig.isAsyncConnect();
            if (!executeConnectAsync) {
                // FIXME
                // DefaultChannelFuture.setUseDeadLockChecker(true);
            }
        }

        webSocketBootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("http-decoder", new HttpResponseDecoder());
                pipeline.addLast("http-encoder", new HttpRequestEncoder());
                pipeline.addLast("httpProcessor", NettyAsyncHttpProvider.this);
            }
        });
    }

    protected HttpClientCodec newHttpClientCodec() {
        if (asyncHttpProviderConfig != null) {
            return new HttpClientCodec(asyncHttpProviderConfig.getMaxInitialLineLength(), asyncHttpProviderConfig.getMaxHeaderSize(), asyncHttpProviderConfig.getMaxChunkSize(), false);

        } else {
            return new HttpClientCodec();
        }
    }
    
    protected void initPlainChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(HTTP_HANDLER, newHttpClientCodec());

        if (config.getRequestCompressionLevel() > 0) {
            pipeline.addLast("deflater", new HttpContentCompressor(config.getRequestCompressionLevel()));
        }

        if (config.isCompressionEnabled()) {
            pipeline.addLast("inflater", new HttpContentDecompressor());
        }
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
        pipeline.addLast("httpProcessor", NettyAsyncHttpProvider.this);
    }

    <T> void constructSSLPipeline(final NettyConnectListener<T> cl) {

        secureBootstrap.handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                try {
                    pipeline.addLast(SSL_HANDLER, new SslHandler(createSSLEngine()));
                } catch (Throwable ex) {
                    abort(cl.future(), ex);
                }

                pipeline.addLast(HTTP_HANDLER, newHttpClientCodec());

                if (config.isCompressionEnabled()) {
                    pipeline.addLast("inflater", new HttpContentDecompressor());
                }
                pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
                pipeline.addLast("httpProcessor", NettyAsyncHttpProvider.this);
            }
        });

        secureWebSocketBootstrap.handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                try {
                    pipeline.addLast(SSL_HANDLER, new SslHandler(createSSLEngine()));
                } catch (Throwable ex) {
                    abort(cl.future(), ex);
                }

                pipeline.addLast("http-decoder", new HttpResponseDecoder());
                pipeline.addLast("http-encoder", new HttpRequestEncoder());
                pipeline.addLast("httpProcessor", NettyAsyncHttpProvider.this);
            }
        });
    }

    private Channel lookupInCache(URI uri, ConnectionPoolKeyStrategy connectionPoolKeyStrategy) {
        final Channel channel = connectionsPool.poll(connectionPoolKeyStrategy.getKey(uri));

        if (channel != null) {
            log.debug("Using cached Channel {}\n for uri {}\n", channel, uri);

            try {
                // Always make sure the channel who got cached support the proper protocol. It could
                // only occurs when a HttpMethod.CONNECT is used against a proxy that require upgrading from http to
                // https.
                return verifyChannelPipeline(channel, uri.getScheme());
            } catch (Exception ex) {
                log.debug(ex.getMessage(), ex);
            }
        }
        return null;
    }

    private SSLEngine createSSLEngine() throws IOException, GeneralSecurityException {
        SSLEngine sslEngine = config.getSSLEngineFactory().newSSLEngine();
        if (sslEngine == null) {
            sslEngine = SslUtils.getSSLEngine();
        }
        return sslEngine;
    }

    private Channel verifyChannelPipeline(Channel channel, String scheme) throws IOException, GeneralSecurityException {

        if (channel.pipeline().get(SSL_HANDLER) != null && HTTP.equalsIgnoreCase(scheme)) {
            channel.pipeline().remove(SSL_HANDLER);
        } else if (channel.pipeline().get(HTTP_HANDLER) != null && HTTP.equalsIgnoreCase(scheme)) {
            return channel;
        } else if (channel.pipeline().get(SSL_HANDLER) == null && isSecure(scheme)) {
            channel.pipeline().addFirst(SSL_HANDLER, new SslHandler(createSSLEngine()));
        }
        return channel;
    }

    protected final <T> void writeRequest(final Channel channel, final AsyncHttpClientConfig config, final NettyResponseFuture<T> future) {
        try {
            // If the channel is dead because it was pooled and the remote server decided to close it, we just let it go and the closeChannel do it's work.
            if (!channel.isOpen() || !channel.isActive()) {
                return;
            }

            HttpRequest nettyRequest = future.getNettyRequest();
            Body body = null;
            if (!nettyRequest.getMethod().equals(HttpMethod.CONNECT)) {
                BodyGenerator bg = future.getRequest().getBodyGenerator();
                if (bg != null) {
                    try {
                        body = bg.createBody();
                    } catch (IOException ex) {
                        throw new IllegalStateException(ex);
                    }
                    long length = body.getContentLength();
                    if (length >= 0) {
                        nettyRequest.headers().set(HttpHeaders.Names.CONTENT_LENGTH, length);
                    } else {
                        nettyRequest.headers().set(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
                    }
                } else if (future.getRequest().getParts() != null) {
                    String contentType = nettyRequest.headers().get(HttpHeaders.Names.CONTENT_TYPE);
                    String length = nettyRequest.headers().get(HttpHeaders.Names.CONTENT_LENGTH);
                    body = new MultipartBody(future.getRequest().getParts(), contentType, length);
                }
            }

            if (future.getAsyncHandler() instanceof TransferCompletionHandler) {

                FluentCaseInsensitiveStringsMap h = new FluentCaseInsensitiveStringsMap();
                for (Map.Entry<String, String> entries : future.getNettyRequest().headers()) {
                        h.add(entries.getKey(), entries.getValue());
                }

                ByteBuf content = nettyRequest instanceof FullHttpRequest? FullHttpRequest.class.cast(nettyRequest).content(): Unpooled.buffer(0);
                TransferCompletionHandler.class.cast(future.getAsyncHandler()).transferAdapter(new NettyTransferAdapter(h, content, future.getRequest().getFile()));
            }

            // Leave it to true.
            // FIXME Yeah... explain why instead of saying the same thing as the code
            if (future.getAndSetWriteHeaders(true)) {
                try {
                    channel.writeAndFlush(nettyRequest, channel.newProgressivePromise()).addListener(new ProgressListener(true, future.getAsyncHandler(), future));
                } catch (Throwable cause) {
                    // FIXME why not notify?
                    log.debug(cause.getMessage(), cause);
                    try {
                        channel.close();
                    } catch (RuntimeException ex) {
                        log.debug(ex.getMessage(), ex);
                    }
                    return;
                }
            }

            if (future.getAndSetWriteBody(true)) {
                if (!future.getNettyRequest().getMethod().equals(HttpMethod.CONNECT)) {

                    if (future.getRequest().getFile() != null) {
                        final File file = future.getRequest().getFile();
                        long fileLength = 0;
                        final RandomAccessFile raf = new RandomAccessFile(file, "r");

                        try {
                            fileLength = raf.length();

                            ChannelFuture writeFuture;
                            if (channel.pipeline().get(SslHandler.class) != null) {
                                writeFuture = channel.write(new ChunkedFile(raf, 0, fileLength, MAX_BUFFERED_BYTES), channel.newProgressivePromise());
                            } else {
                                FileRegion region = new OptimizedFileRegion(raf, 0, fileLength);
                                writeFuture = channel.write(region, channel.newProgressivePromise());
                            }
                            writeFuture.addListener(new ProgressListener(false, future.getAsyncHandler(), future) {
                                public void operationComplete(ChannelProgressiveFuture cf) {
                                    try {
                                        raf.close();
                                    } catch (IOException e) {
                                        log.warn("Failed to close request body: {}", e.getMessage(), e);
                                    }
                                    super.operationComplete(cf);
                                }
                            });
                            channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
                        } catch (IOException ex) {
                            if (raf != null) {
                                try {
                                    raf.close();
                                } catch (IOException e) {
                                }
                            }
                            throw ex;
                        }
                    } else if (future.getRequest().getStreamData() != null || future.getRequest().getBodyGenerator() instanceof InputStreamBodyGenerator) {
                        final InputStream is = future.getRequest().getStreamData() != null? future.getRequest().getStreamData(): InputStreamBodyGenerator.class.cast(future.getRequest().getBodyGenerator()).getInputStream();
                        
                        if (future.getAndSetStreamWasAlreadyConsumed()) {
                            if (is.markSupported())
                                is.reset();
                            else
                                log.warn("Stream has already been consumed and cannot be reset");
                        }

                        ChannelFuture writeFuture = channel.write(new ChunkedStream(is), channel.newProgressivePromise());
                        channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
                        writeFuture.addListener(new ProgressListener(false, future.getAsyncHandler(), future) {
                            public void operationComplete(ChannelProgressiveFuture cf) {
                                try {
                                    is.close();
                                } catch (IOException e) {
                                    log.warn("Failed to close request body: {}", e.getMessage(), e);
                                }
                                super.operationComplete(cf);
                            }
                        });
                        channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
                        
                    } else if (body != null) {

                        ChannelFuture writeFuture;
                        if (channel.pipeline().get(SslHandler.class) == null && body instanceof RandomAccessBody) {
                            BodyFileRegion bodyFileRegion = new BodyFileRegion((RandomAccessBody) body);
                            writeFuture = channel.write(bodyFileRegion, channel.newProgressivePromise());
                        } else {
                            BodyGenerator bg = future.getRequest().getBodyGenerator();
                            BodyChunkedInput bodyChunkedInput = new BodyChunkedInput(body);
                            if (bg instanceof FeedableBodyGenerator) {
                                FeedableBodyGenerator.class.cast(bg).setListener(new FeedListener() {
                                    @Override
                                    public void onContentAdded() {
                                        channel.pipeline().get(ChunkedWriteHandler.class).resumeTransfer();
                                    }
                                });
                            }
                            writeFuture = channel.write(bodyChunkedInput, channel.newProgressivePromise());
                        }

                        final Body b = body;
                        writeFuture.addListener(new ProgressListener(false, future.getAsyncHandler(), future) {
                            public void operationComplete(ChannelProgressiveFuture cf) {
                                try {
                                    b.close();
                                } catch (IOException e) {
                                    log.warn("Failed to close request body: {}", e.getMessage(), e);
                                }
                                super.operationComplete(cf);
                            }
                        });
                        channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
                    }
                }
            }
        } catch (Throwable ioe) {
            try {
                channel.close();
            } catch (RuntimeException ex) {
                log.debug(ex.getMessage(), ex);
            }
        }

        try {
            future.touch();
            int requestTimeout = AsyncHttpProviderUtils.requestTimeout(config, future.getRequest());
            int schedulePeriod = requestTimeout != -1 ? (config.getIdleConnectionTimeoutInMs() != -1 ? Math.min(requestTimeout, config.getIdleConnectionTimeoutInMs()) : requestTimeout) : config.getIdleConnectionTimeoutInMs();

            if (schedulePeriod != -1 && !future.isDone() && !future.isCancelled()) {
                ReaperFuture reaperFuture = new ReaperFuture(future);
                Future<?> scheduledFuture = config.reaper().scheduleAtFixedRate(reaperFuture, 0, schedulePeriod, TimeUnit.MILLISECONDS);
                reaperFuture.setScheduledFuture(scheduledFuture);
                future.setReaperFuture(reaperFuture);
            }
        } catch (RejectedExecutionException ex) {
            abort(future, ex);
        }
    }

    protected final static HttpRequest buildRequest(AsyncHttpClientConfig config, Request request, URI uri, boolean allowConnect, ProxyServer proxyServer) throws IOException {

        String method = request.getMethod();
        if (allowConnect && proxyServer != null && isSecure(uri)) {
            method = HttpMethod.CONNECT.toString();
        }
        return construct(config, request, new HttpMethod(method), uri, proxyServer);
    }

    private static SpnegoEngine getSpnegoEngine() {
        if (spnegoEngine == null)
            spnegoEngine = new SpnegoEngine();
        return spnegoEngine;
    }

    private static HttpRequest construct(AsyncHttpClientConfig config, Request request, HttpMethod m, URI uri, ProxyServer proxyServer) throws IOException {

        String host = null;
        HttpVersion httpVersion;
        String requestUri;
        Map<String, Object> headers = new HashMap<String, Object>();
        ByteBuf content = null;
        boolean webSocket = isWebSocket(uri);

        if (request.getVirtualHost() != null) {
            host = request.getVirtualHost();
        } else {
            host = AsyncHttpProviderUtils.getHost(uri);
    	}

        if (m.equals(HttpMethod.CONNECT)) {
            httpVersion = HttpVersion.HTTP_1_0;
            requestUri = AsyncHttpProviderUtils.getAuthority(uri);
        } else {
            httpVersion = HttpVersion.HTTP_1_1;
            if (proxyServer != null && !(isSecure(uri) && config.isUseRelativeURIsWithSSLProxies()))
                requestUri = uri.toString();
            else if (uri.getRawQuery() != null)
                requestUri = uri.getRawPath() + "?" + uri.getRawQuery();
            else
                requestUri = uri.getRawPath();
        }

        if (webSocket) {
            headers.put(HttpHeaders.Names.UPGRADE, HttpHeaders.Values.WEBSOCKET);
            headers.put(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.UPGRADE);
            headers.put(HttpHeaders.Names.ORIGIN, "http://" + uri.getHost() + ":" + (uri.getPort() == -1 ? isSecure(uri.getScheme()) ? 443 : 80 : uri.getPort()));
            headers.put(HttpHeaders.Names.SEC_WEBSOCKET_KEY, WebSocketUtil.getKey());
            headers.put(HttpHeaders.Names.SEC_WEBSOCKET_VERSION, "13");
        }

        if (host != null) {
            if (request.getVirtualHost() != null || uri.getPort() == -1) {
                headers.put(HttpHeaders.Names.HOST, host);
            } else {
                headers.put(HttpHeaders.Names.HOST, host + ":" + uri.getPort());
            }
        } else {
            host = "127.0.0.1";
        }

        if (!m.equals(HttpMethod.CONNECT)) {
            FluentCaseInsensitiveStringsMap h = request.getHeaders();
            if (h != null) {
                for (Entry<String, List<String>> header : h) {
                    String name = header.getKey();
                    if (!HttpHeaders.Names.HOST.equalsIgnoreCase(name)) {
                        for (String value : header.getValue()) {
                            headers.put(name, value);
                        }
                    }
                }
            }

            if (config.isCompressionEnabled()) {
                headers.put(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
            }
        } else {
            List<String> auth = request.getHeaders().get(HttpHeaders.Names.PROXY_AUTHORIZATION);
            if (isNTLM(auth)) {
                headers.put(HttpHeaders.Names.PROXY_AUTHORIZATION, auth.get(0));
            }
        }
        Realm realm = request.getRealm() != null ? request.getRealm() : config.getRealm();

        if (realm != null && realm.getUsePreemptiveAuth()) {

            String domain = realm.getNtlmDomain();
            if (proxyServer != null && proxyServer.getNtlmDomain() != null) {
                domain = proxyServer.getNtlmDomain();
            }

            String authHost = realm.getNtlmHost();
            if (proxyServer != null && proxyServer.getHost() != null) {
                host = proxyServer.getHost();
            }

            switch (realm.getAuthScheme()) {
            case BASIC:
                headers.put(HttpHeaders.Names.AUTHORIZATION, AuthenticatorUtils.computeBasicAuthentication(realm));
                break;
            case DIGEST:
                if (isNonEmpty(realm.getNonce())) {
                    try {
                        headers.put(HttpHeaders.Names.AUTHORIZATION, AuthenticatorUtils.computeDigestAuthentication(realm));
                    } catch (NoSuchAlgorithmException e) {
                        throw new SecurityException(e);
                    }
                }
                break;
            case NTLM:
                try {
                    String msg = ntlmEngine.generateType1Msg("NTLM " + domain, authHost);
                    headers.put(HttpHeaders.Names.AUTHORIZATION, "NTLM " + msg);
                } catch (NTLMEngineException e) {
                    IOException ie = new IOException();
                    ie.initCause(e);
                    throw ie;
                }
                break;
            case KERBEROS:
            case SPNEGO:
                String challengeHeader = null;
                String server = proxyServer == null ? host : proxyServer.getHost();
                try {
                    challengeHeader = getSpnegoEngine().generateToken(server);
                } catch (Throwable e) {
                    IOException ie = new IOException();
                    ie.initCause(e);
                    throw ie;
                }
                headers.put(HttpHeaders.Names.AUTHORIZATION, "Negotiate " + challengeHeader);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Invalid Authentication " + realm);
            }
        }

        if (!webSocket && !request.getHeaders().containsKey(HttpHeaders.Names.CONNECTION)) {
            headers.put(HttpHeaders.Names.CONNECTION, AsyncHttpProviderUtils.keepAliveHeaderValue(config));
        }

        if (proxyServer != null) {
            if (!request.getHeaders().containsKey("Proxy-Connection")) {
                headers.put("Proxy-Connection", AsyncHttpProviderUtils.keepAliveHeaderValue(config));
            }

            if (proxyServer.getPrincipal() != null) {
                if (isNonEmpty(proxyServer.getNtlmDomain())) {

                    List<String> auth = request.getHeaders().get(HttpHeaders.Names.PROXY_AUTHORIZATION);
                    if (!isNTLM(auth)) {
                        try {
                            String msg = ntlmEngine.generateType1Msg(proxyServer.getNtlmDomain(), proxyServer.getHost());
                            headers.put(HttpHeaders.Names.PROXY_AUTHORIZATION, "NTLM " + msg);
                        } catch (NTLMEngineException e) {
                            IOException ie = new IOException();
                            ie.initCause(e);
                            throw ie;
                        }
                    }
                } else {
                    headers.put(HttpHeaders.Names.PROXY_AUTHORIZATION, AuthenticatorUtils.computeBasicAuthentication(proxyServer));
                }
            }
        }

        // Add default accept headers
        if (!request.getHeaders().containsKey(HttpHeaders.Names.ACCEPT)) {
            headers.put(HttpHeaders.Names.ACCEPT, "*/*");
        }

        String userAgentHeader = request.getHeaders().getFirstValue(HttpHeaders.Names.USER_AGENT);
        if (userAgentHeader != null) {
            headers.put(HttpHeaders.Names.USER_AGENT, userAgentHeader);
        } else if (config.getUserAgent() != null) {
            headers.put(HttpHeaders.Names.USER_AGENT, config.getUserAgent());
        } else {
            headers.put(HttpHeaders.Names.USER_AGENT, AsyncHttpProviderUtils.constructUserAgent(NettyAsyncHttpProvider.class, config));
        }

        boolean hasDeferredContent = false;
        if (!m.equals(HttpMethod.CONNECT)) {
            if (isNonEmpty(request.getCookies())) {
                headers.put(HttpHeaders.Names.COOKIE, CookieEncoder.encodeClientSide(request.getCookies(), config.isRfc6265CookieEncoding()));
            }

            if (!m.equals(HttpMethod.HEAD) && !m.equals(HttpMethod.OPTIONS) && !m.equals(HttpMethod.TRACE)) {

                String bodyCharset = request.getBodyEncoding() == null ? DEFAULT_CHARSET : request.getBodyEncoding();
                
                if (request.getByteData() != null) {
                    headers.put(HttpHeaders.Names.CONTENT_LENGTH, request.getByteData().length);
                    content = Unpooled.wrappedBuffer(request.getByteData());

                } else if (request.getStringData() != null) {
                    byte[] bytes = request.getStringData().getBytes(bodyCharset);
                    headers.put(HttpHeaders.Names.CONTENT_LENGTH, bytes.length);
                    content = Unpooled.wrappedBuffer(bytes);

                } else if (request.getStreamData() != null) {
                    hasDeferredContent = true;
                    headers.put(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);

                } else if (isNonEmpty(request.getParams())) {
                    StringBuilder sb = new StringBuilder();
                    for (final Entry<String, List<String>> paramEntry : request.getParams()) {
                        final String key = paramEntry.getKey();
                        for (final String value : paramEntry.getValue()) {
                            UTF8UrlEncoder.appendEncoded(sb, key);
                            sb.append("=");
                            UTF8UrlEncoder.appendEncoded(sb, value);
                            sb.append("&");
                        }
                    }
                    sb.setLength(sb.length() - 1);
                    byte[] bytes = sb.toString().getBytes(bodyCharset);
                    headers.put(HttpHeaders.Names.CONTENT_LENGTH, bytes.length);
                    content = Unpooled.wrappedBuffer(bytes);

                    if (!request.getHeaders().containsKey(HttpHeaders.Names.CONTENT_TYPE)) {
                        headers.put(HttpHeaders.Names.CONTENT_TYPE, HttpHeaders.Values.APPLICATION_X_WWW_FORM_URLENCODED);
                    }

                } else if (request.getParts() != null) {
                    MultipartRequestEntity mre = AsyncHttpProviderUtils.createMultipartRequestEntity(request.getParts(), request.getHeaders());

                    headers.put(HttpHeaders.Names.CONTENT_TYPE, mre.getContentType());
                    headers.put(HttpHeaders.Names.CONTENT_LENGTH, mre.getContentLength());

                    hasDeferredContent = true;

                } else if (request.getEntityWriter() != null) {
                    int length = getPredefinedContentLength(request, headers);

                    if (length == -1) {
                        length = MAX_BUFFERED_BYTES;
                    }

                    ByteBuf b = Unpooled.buffer(length);
                    request.getEntityWriter().writeEntity(new ByteBufOutputStream(b));
                    headers.put(HttpHeaders.Names.CONTENT_LENGTH, b.writerIndex());
                    content = b;
                } else if (request.getFile() != null) {
                    File file = request.getFile();
                    if (!file.isFile()) {
                        throw new IOException(String.format("File %s is not a file or doesn't exist", file.getAbsolutePath()));
                    }
                    headers.put(HttpHeaders.Names.CONTENT_LENGTH, file.length());
                    hasDeferredContent = true;

                } else if (request.getBodyGenerator() != null) {
                    hasDeferredContent = true;
                }
            }
        }

        HttpRequest nettyRequest;
        if (hasDeferredContent) {
            nettyRequest = new DefaultHttpRequest(httpVersion, m, requestUri);
        } else if (content != null) {
            nettyRequest = new DefaultFullHttpRequest(httpVersion, m, requestUri, content);
        } else {
            nettyRequest = new DefaultFullHttpRequest(httpVersion, m, requestUri);
        }
        for (Entry<String, Object> header: headers.entrySet()) {
            nettyRequest.headers().set(header.getKey(), header.getValue());
        }
        return nettyRequest;
    }

    public void close() {
        isClose.set(true);
        try {
            connectionsPool.destroy();
            openChannels.close();

            for (Channel channel : openChannels) {
                ChannelHandlerContext ctx = channel.pipeline().context(NettyAsyncHttpProvider.class);
                Object attachment = ctx.attr(DEFAULT_ATTRIBUTE).get();
                if (attachment instanceof NettyResponseFuture<?>) {
                    NettyResponseFuture<?> future = (NettyResponseFuture<?>) attachment;
                    future.setReaperFuture(null);
                }
            }

            config.executorService().shutdown();
            config.reaper().shutdown();
            if (this.allowReleaseSocketChannelFactory) {
            	eventLoop.shutdownGracefully();
            }
        } catch (Throwable t) {
            log.warn("Unexpected error on close", t);
        }
    }

    @Override
    public Response prepareResponse(final HttpResponseStatus status, final HttpResponseHeaders headers, final List<HttpResponseBodyPart> bodyParts) {
        return new NettyResponse(status, headers, bodyParts);
    }

    @Override
    public <T> ListenableFuture<T> execute(Request request, final AsyncHandler<T> asyncHandler) throws IOException {
        return doConnect(request, asyncHandler, null, true, executeConnectAsync, false);
    }

    // FIXME Netty 3: only called from nextRequest, useCache, asyncConnect and reclaimCache always passed as true
    private <T> void execute(final Request request, final NettyResponseFuture<T> f) throws IOException {
        doConnect(request, f.getAsyncHandler(), f, true, true, true);
    }

    private <T> ListenableFuture<T> doConnect(final Request request, final AsyncHandler<T> asyncHandler, NettyResponseFuture<T> f, boolean useCache, boolean asyncConnect, boolean reclaimCache) throws IOException {

        if (isClose.get()) {
            throw new IOException("Closed");
        }

        if (request.getUrl().startsWith(WEBSOCKET) && !validateWebSocketRequest(request, asyncHandler)) {
            throw new IOException("WebSocket method must be a GET");
        }

        ProxyServer proxyServer = ProxyUtils.getProxyServer(config, request);
        boolean useProxy = proxyServer != null;
        URI uri;
        if (useRawUrl) {
            uri = request.getRawURI();
        } else {
            uri = request.getURI();
        }
        Channel channel = null;

        if (useCache) {
            if (f != null && f.reuseChannel() && f.channel() != null) {
                channel = f.channel();
            } else {
                URI connectionKeyUri = useProxy ? proxyServer.getURI() : uri;
                channel = lookupInCache(connectionKeyUri, request.getConnectionPoolKeyStrategy());
            }
        }

        boolean useSSl = isSecure(uri) && !useProxy;
        // Can we do anything if that's not the case???
        if (channel != null && channel.isOpen() && channel.isActive()) {
            HttpRequest nettyRequest = null;

            if (f == null) {
            	nettyRequest = buildRequest(config, request, uri, false, proxyServer);
                f = newFuture(uri, request, asyncHandler, nettyRequest, config, this, proxyServer);
            } else {
                nettyRequest = buildRequest(config, request, uri, f.isConnectAllowed(), proxyServer);
                f.setNettyRequest(nettyRequest);
            }
            f.setState(NettyResponseFuture.STATE.POOLED);
            f.attachChannel(channel, false);

            log.debug("\nUsing cached Channel {}\n for request \n{}\n", channel, nettyRequest);
            channel.pipeline().context(NettyAsyncHttpProvider.class).attr(DEFAULT_ATTRIBUTE).set(f);

            try {
                writeRequest(channel, config, f);
            } catch (Exception ex) {
                log.debug("writeRequest failure", ex);
                if (useSSl && ex.getMessage() != null && ex.getMessage().contains("SSLEngine")) {
                    log.debug("SSLEngine failure", ex);
                    f = null;
                } else {
                    try {
                        asyncHandler.onThrowable(ex);
                    } catch (Throwable t) {
                        log.warn("doConnect.writeRequest()", t);
                    }
                    IOException ioe = new IOException(ex.getMessage());
                    ioe.initCause(ex);
                    throw ioe;
                }
            }
            return f;
        }

        // Do not throw an exception when we need an extra connection for a redirect.
        if (!reclaimCache && !connectionsPool.canCacheConnection()) {
            IOException ex = new IOException("Too many connections " + config.getMaxTotalConnections());
            try {
                asyncHandler.onThrowable(ex);
            } catch (Throwable t) {
                log.warn("!connectionsPool.canCacheConnection()", t);
            }
            throw ex;
        }

        boolean acquiredConnection = false;

        if (trackConnections && !reclaimCache) {
            if (!freeConnections.tryAcquire()) {
                IOException ex = new IOException("Too many connections " + config.getMaxTotalConnections());
                try {
                    asyncHandler.onThrowable(ex);
                } catch (Throwable t) {
                    log.warn("!connectionsPool.canCacheConnection()", t);
                }
                throw ex;
            } else {
                acquiredConnection = true;
            }
        }

        NettyConnectListener<T> cl = new NettyConnectListener.Builder<T>(config, request, asyncHandler, f, this).build(uri);
        boolean avoidProxy = ProxyUtils.avoidProxy(proxyServer, uri.getHost());

        if (useSSl) {
            constructSSLPipeline(cl);
        }

        ChannelFuture channelFuture;
        Bootstrap bootstrap = request.getUrl().startsWith(WEBSOCKET) ? (useSSl ? secureWebSocketBootstrap : webSocketBootstrap) : (useSSl ? secureBootstrap : plainBootstrap);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectionTimeoutInMs());

        try {
            InetSocketAddress remoteAddress;
            if (request.getInetAddress() != null) {
                remoteAddress = new InetSocketAddress(request.getInetAddress(), AsyncHttpProviderUtils.getPort(uri));
            } else if (proxyServer == null || avoidProxy) {
                remoteAddress = new InetSocketAddress(AsyncHttpProviderUtils.getHost(uri), AsyncHttpProviderUtils.getPort(uri));
            } else {
                remoteAddress = new InetSocketAddress(proxyServer.getHost(), proxyServer.getPort());
            }

            if (request.getLocalAddress() != null) {
                channelFuture = bootstrap.connect(remoteAddress, new InetSocketAddress(request.getLocalAddress(), 0));
            } else {
                channelFuture = bootstrap.connect(remoteAddress);
            }

        } catch (Throwable t) {
            if (acquiredConnection) {
                freeConnections.release();
            }
            abort(cl.future(), t.getCause() == null ? t : t.getCause());
            return cl.future();
        }

        // FIXME when can we have a direct invokation???
//        boolean directInvokation = !(IN_IO_THREAD.get() && DefaultChannelFuture.isUseDeadLockChecker());
        boolean directInvokation = !IN_IO_THREAD.get();

        // FIXME what does it have to do with the presence of a file?
        if (directInvokation && !asyncConnect && request.getFile() == null) {
            int timeOut = config.getConnectionTimeoutInMs() > 0 ? config.getConnectionTimeoutInMs() : Integer.MAX_VALUE;
            if (!channelFuture.awaitUninterruptibly(timeOut, TimeUnit.MILLISECONDS)) {
                if (acquiredConnection) {
                    freeConnections.release();
                }
                // FIXME false or true?
                channelFuture.cancel(false);
                abort(cl.future(), new ConnectException(String.format("Connect operation to %s timeout %s", uri, timeOut)));
            }

            try {
                cl.operationComplete(channelFuture);
            } catch (Exception e) {
                if (acquiredConnection) {
                    freeConnections.release();
                }
                IOException ioe = new IOException(e.getMessage());
                ioe.initCause(e);
                try {
                    asyncHandler.onThrowable(ioe);
                } catch (Throwable t) {
                    log.warn("c.operationComplete()", t);
                }
                throw ioe;
            }
        } else {
            channelFuture.addListener(cl);
        }

        // FIXME Why non cached???
        log.debug("\nNon cached request \n{}\n\nusing Channel \n{}\n", cl.future().getNettyRequest(), channelFuture.channel());

        if (!cl.future().isCancelled() || !cl.future().isDone()) {
            openChannels.add(channelFuture.channel());
            cl.future().attachChannel(channelFuture.channel(), false);
        }
        return cl.future();
    }

    private void closeChannel(final ChannelHandlerContext ctx) {
        connectionsPool.removeAll(ctx.channel());
        finishChannel(ctx);
    }

    private void finishChannel(final ChannelHandlerContext ctx) {
        ctx.attr(DEFAULT_ATTRIBUTE).set(new DiscardEvent());

        // The channel may have already been removed if a timeout occurred, and this method may be called just after.
        if (ctx.channel() == null) {
            return;
        }

        log.debug("Closing Channel {} ", ctx.channel());

        try {
            ctx.channel().close();
        } catch (Throwable t) {
            log.debug("Error closing a connection", t);
        }

        if (ctx.channel() != null) {
            openChannels.remove(ctx.channel());
        }
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object e) throws Exception {

        // FIXME call to super.channelRead decrease the retain count, have to increase it in order to  use it in Protocol.handle
//        if (e instanceof HttpContent) {
//            HttpContent.class.cast(e).content().retain();
//        }
//
//        // call super to reset the read timeout
//        // FIXME really?
//        super.channelRead(ctx, e);

        IN_IO_THREAD.set(Boolean.TRUE);
        
        Object attachment = ctx.attr(DEFAULT_ATTRIBUTE).get();
        if (attachment == null) {
            log.debug("ChannelHandlerContext wasn't having any attachment");
        }

        if (attachment instanceof DiscardEvent) {
            return;
        } else if (attachment instanceof AsyncCallable) {
            if (e instanceof HttpContent) {
                HttpContent chunk = (HttpContent) e;
                if (chunk instanceof LastHttpContent) {
                    AsyncCallable ac = (AsyncCallable) attachment;
                    ac.call();
                } else {
                    return;
                }
            } else {
                AsyncCallable ac = (AsyncCallable) attachment;
                ac.call();
            }
            ctx.attr(DEFAULT_ATTRIBUTE).set(new DiscardEvent());
            return;
        } else if (!(attachment instanceof NettyResponseFuture<?>)) {
            try {
                ctx.channel().close();
            } catch (Throwable t) {
                log.trace("Closing an orphan channel {}", ctx.channel());
            }
            return;
        }

        Protocol p = (ctx.pipeline().get(HttpClientCodec.class) != null ? httpProtocol : webSocketProtocol);
        p.handle(ctx, e);
    }

    private Realm kerberosChallenge(List<String> proxyAuth, Request request, ProxyServer proxyServer, FluentCaseInsensitiveStringsMap headers, Realm realm, NettyResponseFuture<?> future) throws NTLMEngineException {

        URI uri = request.getURI();
        String host = request.getVirtualHost() == null ? AsyncHttpProviderUtils.getHost(uri) : request.getVirtualHost();
        String server = proxyServer == null ? host : proxyServer.getHost();
        try {
            String challengeHeader = getSpnegoEngine().generateToken(server);
            headers.remove(HttpHeaders.Names.AUTHORIZATION);
            headers.add(HttpHeaders.Names.AUTHORIZATION, "Negotiate " + challengeHeader);

            Realm.RealmBuilder realmBuilder;
            if (realm != null) {
                realmBuilder = new Realm.RealmBuilder().clone(realm);
            } else {
                realmBuilder = new Realm.RealmBuilder();
            }
            return realmBuilder.setUri(uri.getRawPath()).setMethodName(request.getMethod()).setScheme(Realm.AuthScheme.KERBEROS).build();
        } catch (Throwable throwable) {
            if (isNTLM(proxyAuth)) {
                return ntlmChallenge(proxyAuth, request, proxyServer, headers, realm, future);
            }
            abort(future, throwable);
            return null;
        }
    }

	private void addType3NTLMAuthorizationHeader(List<String> auth, FluentCaseInsensitiveStringsMap headers, String username, String password, String domain, String workstation)
	        throws NTLMEngineException {
		headers.remove(HttpHeaders.Names.AUTHORIZATION);

		if (isNTLM(auth)) {
			String serverChallenge = auth.get(0).trim().substring("NTLM ".length());
			String challengeHeader = ntlmEngine.generateType3Msg(username, password, domain, workstation, serverChallenge);

			headers.add(HttpHeaders.Names.AUTHORIZATION, "NTLM " + challengeHeader);
		}
	}

    private Realm ntlmChallenge(List<String> wwwAuth, Request request, ProxyServer proxyServer, FluentCaseInsensitiveStringsMap headers, Realm realm, NettyResponseFuture<?> future) throws NTLMEngineException {

        boolean useRealm = (proxyServer == null && realm != null);

        String ntlmDomain = useRealm ? realm.getNtlmDomain() : proxyServer.getNtlmDomain();
        String ntlmHost = useRealm ? realm.getNtlmHost() : proxyServer.getHost();
        String principal = useRealm ? realm.getPrincipal() : proxyServer.getPrincipal();
        String password = useRealm ? realm.getPassword() : proxyServer.getPassword();

        Realm newRealm;
        if (realm != null && !realm.isNtlmMessageType2Received()) {
            String challengeHeader = ntlmEngine.generateType1Msg(ntlmDomain, ntlmHost);

            URI uri = request.getURI();
            headers.add(HttpHeaders.Names.AUTHORIZATION, "NTLM " + challengeHeader);
            newRealm = new Realm.RealmBuilder().clone(realm).setScheme(realm.getAuthScheme()).setUri(uri.getRawPath()).setMethodName(request.getMethod()).setNtlmMessageType2Received(true).build();
            future.getAndSetAuth(false);
        } else {
        	addType3NTLMAuthorizationHeader(wwwAuth, headers, principal, password, ntlmDomain, ntlmHost);

            Realm.RealmBuilder realmBuilder;
            Realm.AuthScheme authScheme;
            if (realm != null) {
                realmBuilder = new Realm.RealmBuilder().clone(realm);
                authScheme = realm.getAuthScheme();
            } else {
                realmBuilder = new Realm.RealmBuilder();
                authScheme = Realm.AuthScheme.NTLM;
            }
            newRealm = realmBuilder.setScheme(authScheme).setUri(request.getURI().getPath()).setMethodName(request.getMethod()).build();
        }

        return newRealm;
    }

    private Realm ntlmProxyChallenge(List<String> wwwAuth, Request request, ProxyServer proxyServer, FluentCaseInsensitiveStringsMap headers, Realm realm, NettyResponseFuture<?> future) throws NTLMEngineException {
        future.getAndSetAuth(false);
        headers.remove(HttpHeaders.Names.PROXY_AUTHORIZATION);

        addType3NTLMAuthorizationHeader(wwwAuth, headers, proxyServer.getPrincipal(), proxyServer.getPassword(), proxyServer.getNtlmDomain(), proxyServer.getHost());

        Realm newRealm;
        Realm.RealmBuilder realmBuilder;
        if (realm != null) {
            realmBuilder = new Realm.RealmBuilder().clone(realm);
        } else {
            realmBuilder = new Realm.RealmBuilder();
        }
        newRealm = realmBuilder// .setScheme(realm.getAuthScheme())
                .setUri(request.getURI().getPath()).setMethodName(request.getMethod()).build();

        return newRealm;
    }

    private String getPoolKey(NettyResponseFuture<?> future) throws MalformedURLException {
        URI uri = future.getProxyServer() != null ? future.getProxyServer().getURI() : future.getURI();
        return future.getConnectionPoolKeyStrategy().getKey(uri);
    }

    private void drainChannel(final ChannelHandlerContext ctx, final NettyResponseFuture<?> future) {
        ctx.attr(DEFAULT_ATTRIBUTE).set(new AsyncCallable(future) {
            public Object call() throws Exception {
                if (future.isKeepAlive() && ctx.channel().isActive() && connectionsPool.offer(getPoolKey(future), ctx.channel())) {
                    return null;
                }

                finishChannel(ctx);
                return null;
            }

            @Override
            public String toString() {
                return "Draining task for channel " + ctx.channel();
            }
        });
    }

    private FilterContext handleIoException(FilterContext fc, NettyResponseFuture<?> future) {
        for (IOExceptionFilter asyncFilter : config.getIOExceptionFilters()) {
            try {
                fc = asyncFilter.filter(fc);
                if (fc == null) {
                    throw new NullPointerException("FilterContext is null");
                }
            } catch (FilterException efe) {
                abort(future, efe);
            }
        }
        return fc;
    }

    // FIXME Clean up Netty 3: replayRequest's response parameter is unused + WTF return???
    private void replayRequest(final NettyResponseFuture<?> future, FilterContext fc, ChannelHandlerContext ctx) throws IOException {
        Request newRequest = fc.getRequest();
        future.setAsyncHandler(fc.getAsyncHandler());
        future.setState(NettyResponseFuture.STATE.NEW);
        future.touch();

        log.debug("\n\nReplaying Request {}\n for Future {}\n", newRequest, future);
        drainChannel(ctx, future);
        execute(newRequest, future);
    }

    private List<String> getAuthorizationToken(Iterable<Entry<String, String>> list, String headerAuth) {
        ArrayList<String> l = new ArrayList<String>();
        for (Entry<String, String> e : list) {
            if (e.getKey().equalsIgnoreCase(headerAuth)) {
                l.add(e.getValue().trim());
            }
        }
        return l;
    }

    private void abort(NettyResponseFuture<?> future, Throwable t) {
        Channel channel = future.channel();
        if (channel != null && openChannels.contains(channel)) {
            closeChannel(channel.pipeline().context(NettyAsyncHttpProvider.class));
            openChannels.remove(channel);
        }

        if (!future.isCancelled() && !future.isDone()) {
            log.debug("Aborting Future {}\n", future);
            log.debug(t.getMessage(), t);
        }

        future.abort(t);
    }

    private void upgradeProtocol(ChannelPipeline p, String scheme) throws IOException, GeneralSecurityException {
        if (p.get(HTTP_HANDLER) != null) {
            p.remove(HTTP_HANDLER);
        }

        if (isSecure(scheme)) {
            if (p.get(SSL_HANDLER) == null) {
                p.addFirst(HTTP_HANDLER, newHttpClientCodec());
                p.addFirst(SSL_HANDLER, new SslHandler(createSSLEngine()));
            } else {
                p.addAfter(SSL_HANDLER, HTTP_HANDLER, newHttpClientCodec());
            }

        } else {
            p.addFirst(HTTP_HANDLER, newHttpClientCodec());
        }
    }

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        if (isClose.get()) {
            return;
        }

        connectionsPool.removeAll(ctx.channel());
        try {
            super.channelInactive(ctx);
        } catch (Exception ex) {
            log.trace("super.channelClosed", ex);
        }

        Object attachment = ctx.attr(DEFAULT_ATTRIBUTE).get();
        log.debug("Channel Closed: {} with attachment {}", ctx.channel(), attachment);

        if (attachment instanceof AsyncCallable) {
            AsyncCallable ac = (AsyncCallable) attachment;
            ctx.attr(DEFAULT_ATTRIBUTE).set(ac.future());
            ac.call();
            return;
        }

        if (attachment instanceof NettyResponseFuture<?>) {
            NettyResponseFuture<?> future = (NettyResponseFuture<?>) attachment;
            future.touch();

            if (!config.getIOExceptionFilters().isEmpty()) {
                FilterContext<?> fc = new FilterContext.FilterContextBuilder().asyncHandler(future.getAsyncHandler()).request(future.getRequest()).ioException(new IOException("Channel Closed")).build();
                fc = handleIoException(fc, future);

                if (fc.replayRequest() && !future.cannotBeReplay()) {
                    replayRequest(future, fc, ctx);
                    return;
                }
            }

            Protocol p = (ctx.pipeline().get(HttpClientCodec.class) != null ? httpProtocol : webSocketProtocol);
            p.onClose(ctx);

            if (future != null && !future.isDone() && !future.isCancelled()) {
                if (remotelyClosed(ctx.channel(), future)) {
                    abort(future, new IOException("Remotely Closed"));
                }
            } else {
                closeChannel(ctx);
            }
        }
    }

    protected boolean remotelyClosed(Channel channel, NettyResponseFuture<?> future) {

        if (isClose.get()) {
            return true;
        }

        connectionsPool.removeAll(channel);

        if (future == null) {
            Object attachment = channel.pipeline().context(NettyAsyncHttpProvider.class).attr(DEFAULT_ATTRIBUTE).get();
            if (attachment instanceof NettyResponseFuture)
                future = (NettyResponseFuture<?>) attachment;
        }

        if (future == null || future.cannotBeReplay()) {
            log.debug("Unable to recover future {}\n", future);
            return true;
        }

        future.setState(NettyResponseFuture.STATE.RECONNECTED);
        future.getAndSetStatusReceived(false);

        log.debug("Trying to recover request {}\n", future.getNettyRequest());

        try {
            execute(future.getRequest(), future);
            return false;
        } catch (IOException iox) {
            future.setState(NettyResponseFuture.STATE.CLOSED);
            future.abort(iox);
            log.error("Remotely Closed, unable to recover", iox);
        }
        return true;
    }

    private void markAsDone(final NettyResponseFuture<?> future, final ChannelHandlerContext ctx) throws MalformedURLException {
        // We need to make sure everything is OK before adding the connection back to the pool.
        try {
            future.done();
        } catch (Throwable t) {
            // Never propagate exception once we know we are done.
            log.debug(t.getMessage(), t);
        }

        if (!future.isKeepAlive() || !ctx.channel().isActive()) {
            closeChannel(ctx);
        }
    }

    private void finishUpdate(final NettyResponseFuture<?> future, final ChannelHandlerContext ctx, boolean lastValidChunk) throws IOException {
        if (lastValidChunk && future.isKeepAlive()) {
            drainChannel(ctx, future);
        } else {
            if (future.isKeepAlive() && ctx.channel().isActive() && connectionsPool.offer(getPoolKey(future), ctx.channel())) {
                markAsDone(future, ctx);
                return;
            }
            finishChannel(ctx);
        }
        markAsDone(future, ctx);
    }

    private final boolean updateStatusAndInterrupt(AsyncHandler<?> handler, HttpResponseStatus c) throws Exception {
        return handler.onStatusReceived(c) != STATE.CONTINUE;
    }

    private final boolean updateBodyAndInterrupt(final NettyResponseFuture<?> future, AsyncHandler<?> handler, HttpResponseBodyPart c) throws Exception {
        boolean state = handler.onBodyPartReceived(c) != STATE.CONTINUE;
        if (c.closeUnderlyingConnection()) {
            future.setKeepAlive(false);
        }
        return state;
    }

    // Simple marker for stopping publishing bytes.

    final static class DiscardEvent {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
        Channel channel = ctx.channel();
        Throwable cause = e.getCause() != null? e.getCause(): e;
        NettyResponseFuture<?> future = null;

        if (e.getCause() instanceof PrematureChannelClosureException) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Unexpected I/O exception on channel {}", channel, cause);
        }

        try {

            if (cause instanceof ClosedChannelException) {
                return;
            }

            if (ctx.attr(DEFAULT_ATTRIBUTE).get() instanceof NettyResponseFuture<?>) {
                future = (NettyResponseFuture<?>) ctx.attr(DEFAULT_ATTRIBUTE).get();
                future.attachChannel(null, false);
                future.touch();

                if (cause instanceof IOException) {

                    if (!config.getIOExceptionFilters().isEmpty()) {
                        FilterContext<?> fc = new FilterContext.FilterContextBuilder().asyncHandler(future.getAsyncHandler()).request(future.getRequest()).ioException(new IOException("Channel Closed")).build();
                        fc = handleIoException(fc, future);

                        if (fc.replayRequest()) {
                            replayRequest(future, fc, ctx);
                            return;
                        }
                    } else {
                        // Close the channel so the recovering can occurs.
                        try {
                            ctx.channel().close();
                        } catch (Throwable t) {
                            ; // Swallow.
                        }
                        return;
                    }
                }

                if (abortOnReadCloseException(cause) || abortOnWriteCloseException(cause)) {
                    log.debug("Trying to recover from dead Channel: {}", channel);
                    return;
                }
            } else if (ctx.attr(DEFAULT_ATTRIBUTE).get() instanceof AsyncCallable) {
                future = ((AsyncCallable) ctx.attr(DEFAULT_ATTRIBUTE).get()).future();
            }
        } catch (Throwable t) {
            cause = t;
        }

        if (future != null) {
            try {
                log.debug("Was unable to recover Future: {}", future);
                abort(future, cause);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }

        Protocol p = (ctx.pipeline().get(HttpClientCodec.class) != null ? httpProtocol : webSocketProtocol);
        p.onError(ctx, e);

        closeChannel(ctx);
        ctx.fireChannelRead(e);
    }

    protected static boolean abortOnConnectCloseException(Throwable cause) {
        try {
            for (StackTraceElement element : cause.getStackTrace()) {
                if (element.getClassName().equals("sun.nio.ch.SocketChannelImpl") && element.getMethodName().equals("checkConnect")) {
                    return true;
                }
            }

            if (cause.getCause() != null) {
                return abortOnConnectCloseException(cause.getCause());
            }

        } catch (Throwable t) {
        }
        return false;
    }

    protected static boolean abortOnDisconnectException(Throwable cause) {
        try {
            for (StackTraceElement element : cause.getStackTrace()) {
                if (element.getClassName().equals("io.netty.handler.ssl.SslHandler") && element.getMethodName().equals("channelDisconnected")) {
                    return true;
                }
            }

            if (cause.getCause() != null) {
                return abortOnConnectCloseException(cause.getCause());
            }

        } catch (Throwable t) {
        }
        return false;
    }

    protected static boolean abortOnReadCloseException(Throwable cause) {

        for (StackTraceElement element : cause.getStackTrace()) {
            if (element.getClassName().equals("sun.nio.ch.SocketDispatcher") && element.getMethodName().equals("read")) {
                return true;
            }
        }

        if (cause.getCause() != null) {
            return abortOnReadCloseException(cause.getCause());
        }

        return false;
    }

    protected static boolean abortOnWriteCloseException(Throwable cause) {

        for (StackTraceElement element : cause.getStackTrace()) {
            if (element.getClassName().equals("sun.nio.ch.SocketDispatcher") && element.getMethodName().equals("write")) {
                return true;
            }
        }

        if (cause.getCause() != null) {
            return abortOnReadCloseException(cause.getCause());
        }

        return false;
    }

    private final static int getPredefinedContentLength(Request request, Map<String, Object> headers) {
        int length = (int) request.getContentLength();
        Object contentLength = headers.get(HttpHeaders.Names.CONTENT_LENGTH);
        if (length == -1 && contentLength != null) {
            length = Integer.valueOf(contentLength.toString());
        }

        return length;
    }

    public static <T> NettyResponseFuture<T> newFuture(URI uri, Request request, AsyncHandler<T> asyncHandler, HttpRequest nettyRequest, AsyncHttpClientConfig config, NettyAsyncHttpProvider provider, ProxyServer proxyServer) {

        int requestTimeout = AsyncHttpProviderUtils.requestTimeout(config, request);
        NettyResponseFuture<T> f = new NettyResponseFuture<T>(uri,//
                request,//
                asyncHandler,//
                nettyRequest,//
                requestTimeout,//
                config.getIdleConnectionTimeoutInMs(),//
                provider,//
                request.getConnectionPoolKeyStrategy(),//
                proxyServer);

        String expectHeader = request.getHeaders().getFirstValue(HttpHeaders.Names.EXPECT);
        if (expectHeader != null && expectHeader.equalsIgnoreCase(HttpHeaders.Values.CONTINUE)) {
            f.getAndSetWriteBody(false);
        }
        return f;
    }

    private class ProgressListener implements ChannelProgressiveFutureListener {

        private final boolean notifyHeaders;
        private final AsyncHandler<?> asyncHandler;
        private final NettyResponseFuture<?> future;

        public ProgressListener(boolean notifyHeaders, AsyncHandler<?> asyncHandler, NettyResponseFuture<?> future) {
            this.notifyHeaders = notifyHeaders;
            this.asyncHandler = asyncHandler;
            this.future = future;
        }

        @Override
        public void operationComplete(ChannelProgressiveFuture cf) {
            // The write operation failed. If the channel was cached, it means it got asynchronously closed.
            // Let's retry a second time.
            Throwable cause = cf.cause();
            if (cause != null && future.getState() != NettyResponseFuture.STATE.NEW) {

                if (cause instanceof IllegalStateException) {
                    log.debug(cause.getMessage(), cause);
                    try {
                        cf.channel().close();
                    } catch (RuntimeException ex) {
                        log.debug(ex.getMessage(), ex);
                    }
                    return;
                }

                if (cause instanceof ClosedChannelException || abortOnReadCloseException(cause) || abortOnWriteCloseException(cause)) {

                    if (log.isDebugEnabled()) {
                        log.debug(cf.cause() == null ? "" : cf.cause().getMessage(), cf.cause());
                    }

                    try {
                        cf.channel().close();
                    } catch (RuntimeException ex) {
                        log.debug(ex.getMessage(), ex);
                    }
                    return;
                } else {
                    future.abort(cause);
                }
                return;
            }
            future.touch();

            /**
             * We need to make sure we aren't in the middle of an authorization process before publishing events as we will re-publish again the same event after the authorization, causing unpredictable behavior.
             */
            Realm realm = future.getRequest().getRealm() != null ? future.getRequest().getRealm() : NettyAsyncHttpProvider.this.getConfig().getRealm();
            boolean startPublishing = future.isInAuth() || realm == null || realm.getUsePreemptiveAuth();

            if (startPublishing && asyncHandler instanceof ProgressAsyncHandler) {
                if (notifyHeaders) {
                    ProgressAsyncHandler.class.cast(asyncHandler).onHeaderWriteCompleted();
                } else {
                    ProgressAsyncHandler.class.cast(asyncHandler).onContentWriteCompleted();
                }
            }
        }

        @Override
        public void operationProgressed(ChannelProgressiveFuture f, long progress, long total) {
            future.touch();
            if (asyncHandler instanceof ProgressAsyncHandler) {
                ProgressAsyncHandler.class.cast(asyncHandler).onContentWriteProgress(total - progress, progress, total);
            }
        }
    }

    /**
     * Because some implementation of the ThreadSchedulingService do not clean up cancel task until they try to run them, we wrap the task with the future so the when the NettyResponseFuture cancel the reaper future this wrapper will release the references to the channel and the
     * nettyResponseFuture immediately. Otherwise, the memory referenced this way will only be released after the request timeout period which can be arbitrary long.
     */
    private final class ReaperFuture implements Future, Runnable {
        private Future scheduledFuture;
        private NettyResponseFuture<?> nettyResponseFuture;

        public ReaperFuture(NettyResponseFuture<?> nettyResponseFuture) {
            this.nettyResponseFuture = nettyResponseFuture;
        }

        public void setScheduledFuture(Future scheduledFuture) {
            this.scheduledFuture = scheduledFuture;
        }

        /**
         * @Override
         */
        public boolean cancel(boolean mayInterruptIfRunning) {
            nettyResponseFuture = null;
            return scheduledFuture.cancel(mayInterruptIfRunning);
        }

        /**
         * @Override
         */
        public Object get() throws InterruptedException, ExecutionException {
            return scheduledFuture.get();
        }

        /**
         * @Override
         */
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return scheduledFuture.get(timeout, unit);
        }

        /**
         * @Override
         */
        public boolean isCancelled() {
            return scheduledFuture.isCancelled();
        }

        /**
         * @Override
         */
        public boolean isDone() {
            return scheduledFuture.isDone();
        }

        private void expire(String message) {
            log.debug("{} for {}", message, nettyResponseFuture);
            abort(nettyResponseFuture, new TimeoutException(message));
            nettyResponseFuture = null;
        }

        /**
         * @Override
         */
        public synchronized void run() {
            if (isClose.get()) {
                cancel(true);
                return;
            }

            boolean futureDone = nettyResponseFuture.isDone();
            boolean futureCanceled = nettyResponseFuture.isCancelled();

            if (nettyResponseFuture != null && !futureDone && !futureCanceled) {
                long now = millisTime();
                if (nettyResponseFuture.hasRequestTimedOut(now)) {
                    long age = now - nettyResponseFuture.getStart();
                    expire("Request reached time out of " + nettyResponseFuture.getRequestTimeoutInMs() + " ms after " + age + " ms");
                } else if (nettyResponseFuture.hasConnectionIdleTimedOut(now)) {
                    long age = now - nettyResponseFuture.getStart();
                    expire("Request reached idle time out of " + nettyResponseFuture.getIdleConnectionTimeoutInMs() + " ms after " + age + " ms");
                }

            } else if (nettyResponseFuture == null || futureDone || futureCanceled) {
                cancel(true);
            }
        }
    }

    private abstract class AsyncCallable implements Callable<Object> {

        private final NettyResponseFuture<?> future;

        public AsyncCallable(NettyResponseFuture<?> future) {
            this.future = future;
        }

        abstract public Object call() throws Exception;

        public NettyResponseFuture<?> future() {
            return future;
        }
    }

    public static class ThreadLocalBoolean extends ThreadLocal<Boolean> {

        private final boolean defaultValue;

        public ThreadLocalBoolean() {
            this(false);
        }

        public ThreadLocalBoolean(boolean defaultValue) {
            this.defaultValue = defaultValue;
        }

        @Override
        protected Boolean initialValue() {
            return defaultValue ? Boolean.TRUE : Boolean.FALSE;
        }
    }

    public static class OptimizedFileRegion extends AbstractReferenceCounted implements FileRegion {

        private final FileChannel file;
        private final RandomAccessFile raf;
        private final long position;
        private final long count;
        private long byteWritten;

        public OptimizedFileRegion(RandomAccessFile raf, long position, long count) {
            this.raf = raf;
            this.file = raf.getChannel();
            this.position = position;
            this.count = count;
        }

        public long position() {
            return position;
        }

        public long count() {
            return count;
        }
        
        public long transfered() {
            return byteWritten;
        }

        public long transferTo(WritableByteChannel target, long position) throws IOException {
            long count = this.count - position;
            if (count < 0 || position < 0) {
                throw new IllegalArgumentException("position out of range: " + position + " (expected: 0 - " + (this.count - 1) + ")");
            }
            if (count == 0) {
                return 0L;
            }

            long bw = file.transferTo(this.position + position, count, target);
            byteWritten += bw;
            if (byteWritten == raf.length()) {
                deallocate();
            }
            return bw;
        }

        public void deallocate() {
            try {
                file.close();
            } catch (IOException e) {
                log.warn("Failed to close a file.", e);
            }

            try {
                raf.close();
            } catch (IOException e) {
                log.warn("Failed to close a file.", e);
            }
        }
    }

    private static class NettyTransferAdapter extends TransferCompletionHandler.TransferAdapter {

        private final ByteBuf content;
        private final FileInputStream file;
        private int byteRead = 0;

        public NettyTransferAdapter(FluentCaseInsensitiveStringsMap headers, ByteBuf content, File file) throws IOException {
            super(headers);
            this.content = content;
            if (file != null) {
                this.file = new FileInputStream(file);
            } else {
                this.file = null;
            }
        }

        @Override
        public void getBytes(byte[] bytes) {
            if (content.writableBytes() != 0) {
                content.getBytes(byteRead, bytes);
                byteRead += bytes.length;
            } else if (file != null) {
                try {
                    byteRead += file.read(bytes);
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    protected AsyncHttpClientConfig getConfig() {
        return config;
    }

    private static class NonConnectionsPool implements ConnectionsPool<String, Channel> {

        public boolean offer(String uri, Channel connection) {
            return false;
        }

        public Channel poll(String uri) {
            return null;
        }

        public boolean removeAll(Channel connection) {
            return false;
        }

        public boolean canCacheConnection() {
            return true;
        }

        public void destroy() {
        }
    }

    private static final boolean validateWebSocketRequest(Request request, AsyncHandler<?> asyncHandler) {
        if (request.getMethod() != "GET" || !(asyncHandler instanceof WebSocketUpgradeHandler)) {
            return false;
        }
        return true;
    }

    private boolean redirect(Request request, NettyResponseFuture<?> future, HttpResponse response, final ChannelHandlerContext ctx) throws Exception {

        int statusCode = response.getStatus().code();
        boolean redirectEnabled = request.isRedirectOverrideSet() ? request.isRedirectEnabled() : config.isRedirectEnabled();
        if (redirectEnabled && (statusCode == MOVED_PERMANENTLY.code() || statusCode == FOUND.code() || statusCode == SEE_OTHER.code() || statusCode == TEMPORARY_REDIRECT.code())) {

            if (future.incrementAndGetCurrentRedirectCount() < config.getMaxRedirects()) {
                // We must allow 401 handling again.
                future.getAndSetAuth(false);

                String location = response.headers().get(HttpHeaders.Names.LOCATION);
                URI uri = AsyncHttpProviderUtils.getRedirectUri(future.getURI(), location);
                boolean stripQueryString = config.isRemoveQueryParamOnRedirect();
                if (!uri.toString().equals(future.getURI().toString())) {
                    final RequestBuilder nBuilder = stripQueryString ? new RequestBuilder(future.getRequest()).setQueryParameters(null) : new RequestBuilder(future.getRequest());

                    // FIXME what about 307?
                    if (!(statusCode < FOUND.code() || statusCode > SEE_OTHER.code()) && !(statusCode == FOUND.code() && config.isStrict302Handling())) {
                        nBuilder.setMethod("GET");
                    }
                    final boolean initialConnectionKeepAlive = future.isKeepAlive();
                    final String initialPoolKey = getPoolKey(future);
                    future.setURI(uri);
                    String newUrl = uri.toString();
                    if (request.getUrl().startsWith(WEBSOCKET)) {
                        newUrl = newUrl.replace(HTTP, WEBSOCKET);
                    }

                    log.debug("Redirecting to {}", newUrl);
                    for (String cookieStr : future.getHttpResponse().headers().getAll(HttpHeaders.Names.SET_COOKIE)) {
                        for (Cookie c : CookieDecoder.decode(cookieStr)) {
                            nBuilder.addOrReplaceCookie(c);
                        }
                    }

                    for (String cookieStr : future.getHttpResponse().headers().getAll(HttpHeaders.Names.SET_COOKIE2)) {
                        for (Cookie c : CookieDecoder.decode(cookieStr)) {
                            nBuilder.addOrReplaceCookie(c);
                        }
                    }

                    AsyncCallable ac = new AsyncCallable(future) {
                        public Object call() throws Exception {
                            if (initialConnectionKeepAlive && ctx.channel().isActive() && connectionsPool.offer(initialPoolKey, ctx.channel())) {
                                return null;
                            }
                            finishChannel(ctx);
                            return null;
                        }
                    };

                    if (HttpHeaders.isTransferEncodingChunked(response)) {
                        // We must make sure there is no bytes left before executing the next request.
                        ctx.attr(DEFAULT_ATTRIBUTE).set(ac);
                    } else {
                        ac.call();
                    }
                    
                    Request target = nBuilder.setUrl(newUrl).build();
                    future.setRequest(target);
                    execute(target, future);
                    return true;
                }
            } else {
                throw new MaxRedirectException("Maximum redirect reached: " + config.getMaxRedirects());
            }
        }
        return false;
    }

    private final class HttpProtocol implements Protocol {

        @Override
        public void handle(final ChannelHandlerContext ctx, final Object e) throws Exception {
            final NettyResponseFuture<?> future = (NettyResponseFuture<?>) ctx.attr(DEFAULT_ATTRIBUTE).get();
            future.touch();

            // The connect timeout occurred.
            if (future.isCancelled() || future.isDone()) {
                finishChannel(ctx);
                return;
            }

            HttpRequest nettyRequest = future.getNettyRequest();
            AsyncHandler handler = future.getAsyncHandler();
            Request request = future.getRequest();
            ProxyServer proxyServer = future.getProxyServer();
            try {
                if (e instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) e;

                    log.debug("\n\nRequest {}\n\nResponse {}\n", nettyRequest, response);

                    // store the original headers so we can re-send all them to the handler in case of trailing headers
                    future.setHttpResponse(response);
                    future.setIgnoreNextContents(false);

                    int statusCode = response.getStatus().code();

                    String connectionHeaders = response.headers().get(HttpHeaders.Names.CONNECTION);
                    future.setKeepAlive(connectionHeaders == null || !connectionHeaders.equalsIgnoreCase("close"));

                    
                    Realm realm = request.getRealm() != null ? request.getRealm() : config.getRealm();

                    HttpResponseStatus status = new ResponseStatus(future.getURI(), response, NettyAsyncHttpProvider.this);
                    HttpResponseHeaders responseHeaders = new ResponseHeaders(future.getURI(), response.headers(), NettyAsyncHttpProvider.this);
                    FilterContext fc = new FilterContext.FilterContextBuilder().asyncHandler(handler).request(request).responseStatus(status).responseHeaders(responseHeaders).build();

                    for (ResponseFilter asyncFilter : config.getResponseFilters()) {
                        try {
                            fc = asyncFilter.filter(fc);
                            if (fc == null) {
                                throw new NullPointerException("FilterContext is null");
                            }
                        } catch (FilterException efe) {
                            abort(future, efe);
                        }
                    }

                    // The handler may have been wrapped.
                    handler = fc.getAsyncHandler();
                    future.setAsyncHandler(handler);

                    // The request has changed
                    if (fc.replayRequest()) {
                        replayRequest(future, fc, ctx);
                        future.setIgnoreNextContents(true);
                        return;
                    }

                    final FluentCaseInsensitiveStringsMap headers = request.getHeaders();
                    final RequestBuilder builder = new RequestBuilder(future.getRequest());

                    if (statusCode == UNAUTHORIZED.code() && realm != null) {
                        List<String> wwwAuth = getAuthorizationToken(response.headers(), HttpHeaders.Names.WWW_AUTHENTICATE);
                        if (!wwwAuth.isEmpty() && !future.getAndSetAuth(true)) {
                            future.setState(NettyResponseFuture.STATE.NEW);
                            Realm newRealm = null;
                            // NTLM
                            boolean negociate = wwwAuth.contains("Negotiate");
                            if (!wwwAuth.contains("Kerberos") && (isNTLM(wwwAuth) || negociate)) {
                                newRealm = ntlmChallenge(wwwAuth, request, proxyServer, headers, realm, future);
                                // SPNEGO KERBEROS
                            } else if (negociate) {
                                newRealm = kerberosChallenge(wwwAuth, request, proxyServer, headers, realm, future);
                                if (newRealm == null) {
                                    future.setIgnoreNextContents(true);
                                    return;
                                }
                            } else {
                                newRealm = new Realm.RealmBuilder().clone(realm).setScheme(realm.getAuthScheme()).setUri(request.getURI().getPath()).setMethodName(request.getMethod()).setUsePreemptiveAuth(true).parseWWWAuthenticateHeader(wwwAuth.get(0)).build();
                            }
    
                            final Realm nr = new Realm.RealmBuilder().clone(newRealm).setUri(URI.create(request.getUrl()).getPath()).build();
    
                            log.debug("Sending authentication to {}", request.getUrl());
                            AsyncCallable ac = new AsyncCallable(future) {
                                public Object call() throws Exception {
                                    drainChannel(ctx, future);
                                    execute(builder.setHeaders(headers).setRealm(nr).build(), future);
                                    return null;
                                }
                            };
    
                            if (future.isKeepAlive() && HttpHeaders.isTransferEncodingChunked(response)) {
                                // We must make sure there is no bytes left before executing the next request.
                                ctx.attr(DEFAULT_ATTRIBUTE).set(ac);
                            } else {
                                ac.call();
                            }
                            future.setIgnoreNextContents(true);
                            return;
                        }
                    }

                    if (statusCode == CONTINUE.code()) {
                        future.getAndSetWriteHeaders(false);
                        future.getAndSetWriteBody(true);
                        future.setIgnoreNextContents(true);
                        writeRequest(ctx.channel(), config, future);
                        return;
                    }

                    if (statusCode == PROXY_AUTHENTICATION_REQUIRED.code()) {
                        List<String> proxyAuth = getAuthorizationToken(response.headers(), HttpHeaders.Names.PROXY_AUTHENTICATE);
                        if (realm != null && !proxyAuth.isEmpty() && !future.getAndSetAuth(true)) {
                            log.debug("Sending proxy authentication to {}", request.getUrl());
    
                            future.setState(NettyResponseFuture.STATE.NEW);
                            Realm newRealm = null;

                            boolean negociate = proxyAuth.contains("Negotiate");
                            if (!proxyAuth.contains("Kerberos") && (isNTLM(proxyAuth) || negociate)) {
                                newRealm = ntlmProxyChallenge(proxyAuth, request, proxyServer, headers, realm, future);
                                // SPNEGO KERBEROS
                            } else if (negociate) {
                                newRealm = kerberosChallenge(proxyAuth, request, proxyServer, headers, realm, future);
                                if (newRealm == null) {
                                    future.setIgnoreNextContents(true);
                                    return;
                                }
                            } else {
                                newRealm = future.getRequest().getRealm();
                            }
    
                            Request req = builder.setHeaders(headers).setRealm(newRealm).build();
                            future.setReuseChannel(true);
                            future.setConnectAllowed(true);
                            future.setIgnoreNextContents(true);
                            execute(req, future);
                            return;
                        }
                    }

                    if (statusCode == OK.code() && nettyRequest.getMethod().equals(HttpMethod.CONNECT)) {

                        log.debug("Connected to {}:{}", proxyServer.getHost(), proxyServer.getPort());

                        if (future.isKeepAlive()) {
                            future.attachChannel(ctx.channel(), true);
                        }

                        try {
                            log.debug("Connecting to proxy {} for scheme {}", proxyServer, request.getUrl());
                            upgradeProtocol(ctx.channel().pipeline(), request.getURI().getScheme());
                        } catch (Throwable ex) {
                            abort(future, ex);
                        }
                        Request req = builder.build();
                        future.setReuseChannel(true);
                        future.setConnectAllowed(false);
                        future.setIgnoreNextContents(true);
                        execute(req, future);
                        return;
                    }

                    if (redirect(request, future, response, ctx)) {
                        future.setIgnoreNextContents(true);
                        return;
                    }

                    if (!future.getAndSetStatusReceived(true) && updateStatusAndInterrupt(handler, status)) {
                        finishUpdate(future, ctx, HttpHeaders.isTransferEncodingChunked(response));
                        return;
                    } else if (handler.onHeadersReceived(responseHeaders) != STATE.CONTINUE) {
                        finishUpdate(future, ctx, HttpHeaders.isTransferEncodingChunked(response));
                        return;
                    }
                }

                if (e instanceof HttpContent && !future.isIgnoreNextContents()) {
                    HttpContent chunk = (HttpContent) e;

                    try {
                        if (handler != null) {
                            boolean interrupt = false;
                            boolean last = chunk instanceof LastHttpContent;

                            // FIXME
                            // Netty 3 provider is broken: in case of trailing headers, onHeadersReceived should be called before updateBodyAndInterrupt
                            if (last) {
                                LastHttpContent lastChunk = (LastHttpContent) chunk;
                                HttpHeaders trailingHeaders = lastChunk.trailingHeaders();
                                if (!trailingHeaders.isEmpty()) {
                                    interrupt = handler.onHeadersReceived(new ResponseHeaders(future.getURI(), future.getHttpResponse().headers(), NettyAsyncHttpProvider.this, trailingHeaders)) != STATE.CONTINUE;
                                }
                            }

                            if (!interrupt && chunk.content().readableBytes() > 0) {
                                interrupt = updateBodyAndInterrupt(future, handler, new ResponseBodyPart(future.getURI(), NettyAsyncHttpProvider.this, chunk));
                            }

                            if (interrupt || last) {
                                finishUpdate(future, ctx, !last);
                            }
                        }
                    } finally {
                        chunk.release();
                    }
                }
            } catch (Exception t) {
                if (t instanceof IOException && !config.getIOExceptionFilters().isEmpty()) {
                    FilterContext<?> fc = new FilterContext.FilterContextBuilder().asyncHandler(future.getAsyncHandler()).request(future.getRequest()).ioException(IOException.class.cast(t)).build();
                    fc = handleIoException(fc, future);

                    if (fc.replayRequest()) {
                        replayRequest(future, fc, ctx);
                        return;
                    }
                }

                try {
                    abort(future, t);
                } finally {
                    finishUpdate(future, ctx, false);
                    throw t;
                }
            }
        }

        @Override
        public void onError(ChannelHandlerContext ctx, Throwable error) {
        }

        @Override
        public void onClose(ChannelHandlerContext ctx) {
        }
    }

    private final class WebSocketProtocol implements Protocol {
        private static final byte OPCODE_TEXT = 0x1;
        private static final byte OPCODE_BINARY = 0x2;
        private static final byte OPCODE_UNKNOWN = -1;
        protected byte pendingOpcode = OPCODE_UNKNOWN;

        // We don't need to synchronize as replacing the "ws-decoder" will process using the same thread.
        private void invokeOnSucces(ChannelHandlerContext ctx, WebSocketUpgradeHandler h) {
            if (!h.touchSuccess()) {
                try {
                    h.onSuccess(new NettyWebSocket(ctx.channel()));
                } catch (Exception ex) {
                    NettyAsyncHttpProvider.this.log.warn("onSuccess unexexpected exception", ex);
                }
            }
        }

        @Override
        public void handle(ChannelHandlerContext ctx, Object e) throws Exception {
            Object attachment = ctx.attr(DEFAULT_ATTRIBUTE).get();
            NettyResponseFuture future = NettyResponseFuture.class.cast(attachment);
            WebSocketUpgradeHandler h = WebSocketUpgradeHandler.class.cast(future.getAsyncHandler());
            Request request = future.getRequest();

            if (e instanceof HttpResponse) {
                HttpResponse response = (HttpResponse) e;

                HttpResponseStatus s = new ResponseStatus(future.getURI(), response, NettyAsyncHttpProvider.this);
                HttpResponseHeaders responseHeaders = new ResponseHeaders(future.getURI(), response.headers(), NettyAsyncHttpProvider.this);
                FilterContext<?> fc = new FilterContext.FilterContextBuilder().asyncHandler(h).request(request).responseStatus(s).responseHeaders(responseHeaders).build();
                for (ResponseFilter asyncFilter : config.getResponseFilters()) {
                    try {
                        fc = asyncFilter.filter(fc);
                        if (fc == null) {
                            throw new NullPointerException("FilterContext is null");
                        }
                    } catch (FilterException efe) {
                        abort(future, efe);
                    }

                }

                // The handler may have been wrapped.
                future.setAsyncHandler(fc.getAsyncHandler());

                // The request has changed
                if (fc.replayRequest()) {
                    replayRequest(future, fc, ctx);
                    return;
                }

                future.setHttpResponse(response);
                if (redirect(request, future, response, ctx))
                    return;

                final io.netty.handler.codec.http.HttpResponseStatus status = io.netty.handler.codec.http.HttpResponseStatus.SWITCHING_PROTOCOLS;

                final boolean validStatus = response.getStatus().equals(status);
                final boolean validUpgrade = response.headers().get(HttpHeaders.Names.UPGRADE) != null;
                String c = response.headers().get(HttpHeaders.Names.CONNECTION);
                if (c == null) {
                    c = response.headers().get(HttpHeaders.Names.CONNECTION.toLowerCase());
                }

                final boolean validConnection = c == null ? false : c.equalsIgnoreCase(HttpHeaders.Values.UPGRADE);

                s = new ResponseStatus(future.getURI(), response, NettyAsyncHttpProvider.this);
                final boolean statusReceived = h.onStatusReceived(s) == STATE.UPGRADE;

                final boolean headerOK = h.onHeadersReceived(responseHeaders) == STATE.CONTINUE;
                if (!headerOK || !validStatus || !validUpgrade || !validConnection || !statusReceived) {
                    abort(future, new IOException("Invalid handshake response"));
                    return;
                }

                String accept = response.headers().get(HttpHeaders.Names.SEC_WEBSOCKET_ACCEPT);
                String key = WebSocketUtil.getAcceptKey(future.getNettyRequest().headers().get(HttpHeaders.Names.SEC_WEBSOCKET_KEY));
                if (accept == null || !accept.equals(key)) {
                    throw new IOException(String.format("Invalid challenge. Actual: %s. Expected: %s", accept, key));
                }

                ctx.pipeline().replace("http-encoder", "ws-encoder", new WebSocket08FrameEncoder(true));
//                ctx.pipeline().get(HttpResponseDecoder.class).replace("ws-decoder", new WebSocket08FrameDecoder(false, false));
                // FIXME Right way? Which maxFramePayloadLength? Configurable I guess
                ctx.pipeline().replace(HttpResponseDecoder.class, "ws-decoder", new WebSocket08FrameDecoder(false, false, 10 * 1024));

                invokeOnSucces(ctx, h);
                future.done();

            } else if (e instanceof WebSocketFrame) {

                invokeOnSucces(ctx, h);

                final WebSocketFrame frame = (WebSocketFrame) e;

                if (frame instanceof TextWebSocketFrame) {
                    pendingOpcode = OPCODE_TEXT;
                } else if (frame instanceof BinaryWebSocketFrame) {
                    pendingOpcode = OPCODE_BINARY;
                }

                if (frame.content() != null && frame.content().readableBytes() > 0) {
                    HttpContent webSocketChunk = new DefaultHttpContent(Unpooled.wrappedBuffer(frame.content()));
                    ResponseBodyPart rp = new ResponseBodyPart(future.getURI(), NettyAsyncHttpProvider.this, webSocketChunk);
                    h.onBodyPartReceived(rp);

                    NettyWebSocket webSocket = NettyWebSocket.class.cast(h.onCompleted());

                    if (webSocket != null) {
                        if (pendingOpcode == OPCODE_BINARY) {
                            webSocket.onBinaryFragment(rp.getBodyPartBytes(), frame.isFinalFragment());
                        } else {
                            webSocket.onTextFragment(frame.content().toString(UTF8), frame.isFinalFragment());
                        }

                        if (frame instanceof CloseWebSocketFrame) {
                            try {
                                ctx.attr(DEFAULT_ATTRIBUTE).set(DiscardEvent.class);
                                webSocket.onClose(CloseWebSocketFrame.class.cast(frame).statusCode(), CloseWebSocketFrame.class.cast(frame).reasonText());
                            } catch (Throwable t) {
                                // Swallow any exception that may comes from a Netty version released before 3.4.0
                                log.trace("", t);
                            }
                        }
                    } else {
                        log.debug("UpgradeHandler returned a null NettyWebSocket ");
                    }
                }
            } else {
                log.error("Invalid attachment {}", attachment);
            }
        }

        @Override
        public void onError(ChannelHandlerContext ctx, Throwable e) {
            try {
                Object attachment = ctx.attr(DEFAULT_ATTRIBUTE).get();
                log.warn("onError {}", e);
                if (!(attachment instanceof NettyResponseFuture)) {
                    return;
                }

                NettyResponseFuture<?> nettyResponse = (NettyResponseFuture) attachment;
                WebSocketUpgradeHandler h = WebSocketUpgradeHandler.class.cast(nettyResponse.getAsyncHandler());

                NettyWebSocket webSocket = NettyWebSocket.class.cast(h.onCompleted());
                if (webSocket != null) {
                    webSocket.onError(e.getCause());
                    webSocket.close();
                }
            } catch (Throwable t) {
                log.error("onError", t);
            }
        }

        @Override
        public void onClose(ChannelHandlerContext ctx) {
            log.trace("onClose {}");
            Object attachment = ctx.attr(DEFAULT_ATTRIBUTE).get();
            if (!(attachment instanceof NettyResponseFuture)) {
                return;
            }

            try {
                NettyResponseFuture<?> nettyResponse = NettyResponseFuture.class.cast(attachment);
                WebSocketUpgradeHandler h = WebSocketUpgradeHandler.class.cast(nettyResponse.getAsyncHandler());
                NettyWebSocket webSocket = NettyWebSocket.class.cast(h.onCompleted());

                // FIXME How could this test not succeed, attachment is a NettyResponseFuture????
                if (!(attachment instanceof DiscardEvent))
                    webSocket.close(1006, "Connection was closed abnormally (that is, with no close frame being sent).");
            } catch (Throwable t) {
                log.error("onError", t);
            }
        }
    }

    private static boolean isWebSocket(URI uri) {
        return WEBSOCKET.equalsIgnoreCase(uri.getScheme()) || WEBSOCKET_SSL.equalsIgnoreCase(uri.getScheme());
    }

    private static boolean isSecure(String scheme) {
        return HTTPS.equalsIgnoreCase(scheme) || WEBSOCKET_SSL.equalsIgnoreCase(scheme);
    }

    private static boolean isSecure(URI uri) {
        return isSecure(uri.getScheme());
    }
}
