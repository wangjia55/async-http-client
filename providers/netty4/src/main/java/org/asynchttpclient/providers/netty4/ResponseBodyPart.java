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
 */
package org.asynchttpclient.providers.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.asynchttpclient.AsyncHttpProvider;
import org.asynchttpclient.HttpResponseBodyPart;

/**
 * A callback class used when an HTTP response body is received.
 */
public class ResponseBodyPart extends HttpResponseBodyPart {

    private final HttpContent chunk;
    private final AtomicReference<byte[]> bytes = new AtomicReference<byte[]>(null);
    private final boolean isLast;
    private boolean closeConnection = false;

    public ResponseBodyPart(URI uri, AsyncHttpProvider provider, HttpContent chunk) {
        super(uri, provider);
        this.chunk = chunk;
        isLast = chunk instanceof LastHttpContent;
    }
    
    /**
     * Return the response body's part bytes received.
     *
     * @return the response body's part bytes received.
     */
    @Override
    public byte[] getBodyPartBytes() {
        byte[] bp = bytes.get();
        if (bp != null) {
            return bp;
        }

        ByteBuf b = getChannelBuffer();
        byte[] rb = b.nioBuffer().array();
        bytes.set(rb);
        return rb;
    }

    @Override
    public InputStream readBodyPartBytes() {
        return new ByteArrayInputStream(getBodyPartBytes());
    }

    @Override
    public int length() {
        return chunk.content().readableBytes();
    }
    
    @Override
    public int writeTo(OutputStream outputStream) throws IOException {
        ByteBuf b = getChannelBuffer();
        int available = b.readableBytes();
        if (available > 0) {
            b.getBytes(b.readerIndex(), outputStream, available);
        }
        return available;
    }

    @Override
    public ByteBuffer getBodyByteBuffer() {
        return ByteBuffer.wrap(getBodyPartBytes());
    }

    public ByteBuf getChannelBuffer() {
        return chunk.content();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLast() {
        return isLast;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void markUnderlyingConnectionAsClosed() {
        closeConnection = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean closeUnderlyingConnection() {
        return closeConnection;
    }

    protected HttpContent chunk() {
        return chunk;
    }
}
