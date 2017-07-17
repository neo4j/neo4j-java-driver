/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.TLSSocketChannel;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;

/**
 * This tests that the TLSSocketChannel handles every combination of network buffer sizes that we
 * can reasonably expect to see in the wild. It exhaustively tests power-of-two sizes up to 2^16
 * for the following variables:
 * <p>
 * - Network frame size
 * - Bolt message size
 * - write buffer size
 * <p>
 * It tests every possible combination, and it does this currently only for the read path, expanding
 * to the write path as well would be useful. For each size, it sets up a TLS server and tests the
 * handshake, transferring the data, and verifying the data is correct after decryption.
 */
public class TLSSocketChannelWriteFragmentationIT extends TLSSocketChannelFragmentation
{
    @Override
    protected void testForBufferSizes( byte[] blobOfData, int networkFrameSize, int userBufferSize ) throws Exception
    {
        SSLEngine engine = sslCtx.createSSLEngine();
        engine.setUseClientMode( true );
        SocketAddress address = new InetSocketAddress( serverSocket.getInetAddress(), serverSocket.getLocalPort() );
        ByteChannel ch = new LittleAtATimeChannel( SocketChannel.open( address ), networkFrameSize );

        try ( TLSSocketChannel channel = TLSSocketChannel.create( ch, DEV_NULL_LOGGER, engine, LOCAL_DEFAULT ) )
        {
            ByteBuffer writeBuffer = ByteBuffer.wrap( blobOfData );
            while ( writeBuffer.position() < writeBuffer.capacity() )
            {
                writeBuffer.limit( Math.min( writeBuffer.capacity(), writeBuffer.position() + userBufferSize ) );
                int remainingBytes = writeBuffer.remaining();
                assertEquals( remainingBytes, channel.write( writeBuffer ) );
            }
        }
    }

    @Override
    protected Runnable createServerRunnable( SSLContext sslContext ) throws IOException
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    // noinspection InfiniteLoopStatement
                    while ( true )
                    {
                        Socket client = accept( serverSocket );
                        if ( client == null )
                        {
                            return;
                        }

                        InputStream inputStream = client.getInputStream();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();

                        int read;
                        while ( (read = inputStream.read()) != -1 )
                        {
                            baos.write( read );
                        }

                        assertThat( blobOfData( baos.size() ), equalTo( baos.toByteArray() ) );

                        client.close();
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }
}
