/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.v1.integration;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * This tests that the TLSSocketChannel handles every combination of network buffer sizes that we
 * can reasonably expect to see in the wild. It exhaustively tests power-of-two sizes up to 2^16
 * for the following variables:
 *
 * - Network frame size
 * - Bolt message size
 * - Read buffer size
 *
 * It tests every possible combination, and it does this currently only for the read path, expanding
 * to the write path as well would be useful. For each size, it sets up a TLS server and tests the
 * handshake, transferring the data, and verifying the data is correct after decryption.
 */
public abstract class TLSSocketChannelFragmentation
{
    protected SSLContext sslCtx;

    @Before
    public void setup() throws Throwable
    {
        createSSLContext();
        createServer();
    }

    @Test
    public void shouldHandleFuzziness() throws Throwable
    {
        // Given
        int networkFrameSize, userBufferSize, blobOfDataSize;

        for(int dataBlobMagnitude = 1; dataBlobMagnitude < 16; dataBlobMagnitude+=2 )
        {
            blobOfDataSize = (int) Math.pow( 2, dataBlobMagnitude );

            for ( int frameSizeMagnitude = 1; frameSizeMagnitude < 16; frameSizeMagnitude+=2 )
            {
                networkFrameSize = (int) Math.pow( 2, frameSizeMagnitude );
                for ( int userBufferMagnitude = 1; userBufferMagnitude < 16; userBufferMagnitude+=2 )
                {
                    userBufferSize = (int) Math.pow( 2, userBufferMagnitude );
                    testForBufferSizes( blobOfDataSize, networkFrameSize, userBufferSize );
                }
            }
        }
    }

    protected void createSSLContext()
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException, KeyManagementException
    {
        KeyStore ks = KeyStore.getInstance("JKS");
        char[] password = "password".toCharArray();
        ks.load( getClass().getResourceAsStream( "/keystore.jks" ), password );
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, password);

        sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init( kmf.getKeyManagers(), new TrustManager[]{new X509TrustManager() {
            public void checkClientTrusted( X509Certificate[] chain, String authType) throws CertificateException
            {
            }

            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            }

            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        }}, null );
    }

    protected abstract void testForBufferSizes( int blobOfDataSize, int networkFrameSize, int userBufferSize ) throws IOException,
            GeneralSecurityException;

    protected abstract void createServer() throws IOException;

    /**
     * Delegates to underlying channel, but only reads up to the set amount at a time, used to emulate
     * different network frame sizes in this test.
     */
    protected static class LittleAtATimeChannel implements ByteChannel
    {
        private final ByteChannel delegate;
        private final int maxFrameSize;

        public LittleAtATimeChannel( ByteChannel delegate, int maxFrameSize )
        {

            this.delegate = delegate;
            this.maxFrameSize = maxFrameSize;
        }

        @Override
        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        @Override
        public void close() throws IOException
        {
            delegate.close();
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            int originalLimit = src.limit();
            try
            {
                src.limit( Math.min( src.limit(), src.position() + maxFrameSize ) );
                return delegate.write( src );
            }
            finally
            {
                src.limit(originalLimit);
            }
        }

        @Override
        public int read( ByteBuffer dst ) throws IOException
        {
            int originalLimit = dst.limit();
            try
            {
                dst.limit( Math.min( dst.limit(), dst.position() + maxFrameSize ) );
                return delegate.read( dst );
            }
            finally
            {
                dst.limit(originalLimit);
            }
        }
    }
}
