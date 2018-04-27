/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.util.DaemonThreadFactory.daemon;

/**
 * This tests that the TLSSocketChannel handles every combination of network buffer sizes that we
 * can reasonably expect to see in the wild. It exhaustively tests power-of-two sizes up to 2^16
 * for the following variables:
 * <p>
 * - Network frame size
 * - Bolt message size
 * - Read buffer size
 * <p>
 * It tests every possible combination, and it does this currently only for the read path, expanding
 * to the write path as well would be useful. For each size, it sets up a TLS server and tests the
 * handshake, transferring the data, and verifying the data is correct after decryption.
 */
public abstract class TLSSocketChannelFragmentation
{
    SSLContext sslCtx;
    ServerSocket serverSocket;
    volatile byte[] blobOfData;

    private ExecutorService serverExecutor;
    private Future<?> serverTask;

    @Before
    public void setUp() throws Throwable
    {
        sslCtx = createSSLContext();
        serverSocket = createServerSocket( sslCtx );
        serverExecutor = createServerExecutor();
        serverTask = launchServer( serverExecutor, createServerRunnable( sslCtx ) );
    }

    @After
    public void tearDown() throws Exception
    {
        serverSocket.close();
        serverExecutor.shutdownNow();
        assertTrue( "Unable to terminate server socket", serverExecutor.awaitTermination( 30, SECONDS ) );

        assertNull( serverTask.get( 30, SECONDS ) );
    }

    @Test
    public void shouldHandleFuzziness() throws Throwable
    {
        // Given
        int networkFrameSize, userBufferSize, blobOfDataSize;

        for ( int dataBlobMagnitude = 1; dataBlobMagnitude < 16; dataBlobMagnitude += 2 )
        {
            blobOfDataSize = (int) Math.pow( 2, dataBlobMagnitude );
            blobOfData = blobOfData( blobOfDataSize );

            for ( int frameSizeMagnitude = 1; frameSizeMagnitude < 16; frameSizeMagnitude += 2 )
            {
                networkFrameSize = (int) Math.pow( 2, frameSizeMagnitude );
                for ( int userBufferMagnitude = 1; userBufferMagnitude < 16; userBufferMagnitude += 2 )
                {
                    userBufferSize = (int) Math.pow( 2, userBufferMagnitude );
                    testForBufferSizes( blobOfData, networkFrameSize, userBufferSize );
                }
            }
        }
    }

    protected abstract void testForBufferSizes( byte[] blobOfData, int networkFrameSize, int userBufferSize )
            throws Exception;

    protected abstract Runnable createServerRunnable( SSLContext sslContext ) throws IOException;

    private static SSLContext createSSLContext() throws Exception
    {
        KeyStore ks = KeyStore.getInstance( "JKS" );
        char[] password = "password".toCharArray();
        ks.load( TLSSocketChannelFragmentation.class.getResourceAsStream( "/keystore.jks" ), password );
        KeyManagerFactory kmf = KeyManagerFactory.getInstance( "SunX509" );
        kmf.init( ks, password );

        SSLContext sslCtx = SSLContext.getInstance( "TLS" );
        sslCtx.init( kmf.getKeyManagers(), new TrustManager[]{new X509TrustManager()
        {
            @Override
            public void checkClientTrusted( X509Certificate[] chain, String authType ) throws CertificateException
            {
            }

            @Override
            public void checkServerTrusted( X509Certificate[] chain, String authType ) throws CertificateException
            {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers()
            {
                return null;
            }
        }}, null );

        return sslCtx;
    }

    private static ServerSocket createServerSocket( SSLContext sslContext ) throws IOException
    {
        SSLServerSocketFactory ssf = sslContext.getServerSocketFactory();
        return ssf.createServerSocket( 0 );
    }

    private ExecutorService createServerExecutor()
    {
        return newSingleThreadExecutor( daemon( getClass().getSimpleName() + "-Server-" ) );
    }

    private Future<?> launchServer( ExecutorService executor, Runnable runnable )
    {
        return executor.submit( runnable );
    }

    static byte[] blobOfData( int dataBlobSize )
    {
        byte[] blobOfData = new byte[dataBlobSize];
        // If the blob is all zeros, we'd miss data corruption problems in assertions, so
        // fill the data blob with different values.
        for ( int i = 0; i < blobOfData.length; i++ )
        {
            blobOfData[i] = (byte) (i % 128);
        }

        return blobOfData;
    }

    static Socket accept( ServerSocket serverSocket ) throws IOException
    {
        try
        {
            return serverSocket.accept();
        }
        catch ( SocketException e )
        {
            String message = e.getMessage();
            if ( "Socket closed".equalsIgnoreCase( message ) )
            {
                return null;
            }
            throw e;
        }
    }

    /**
     * Delegates to underlying channel, but only reads up to the set amount at a time, used to emulate
     * different network frame sizes in this test.
     */
    protected static class LittleAtATimeChannel implements ByteChannel
    {
        private final ByteChannel delegate;
        private final int maxFrameSize;

        LittleAtATimeChannel( ByteChannel delegate, int maxFrameSize )
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
                src.limit( originalLimit );
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
                dst.limit( originalLimit );
            }
        }
    }
}
