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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;

import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.security.TLSSocketChannel;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * This tests that the TLSSocketChannel handles every combination of network buffer sizes that we
 * can reasonably expect to see in the wild. It exhaustively tests power-of-two sizes up to 2^16
 * for the following variables:
 *
 * - Network frame size
 * - Bolt message size
 * - write buffer size
 *
 * It tests every possible combination, and it does this currently only for the read path, expanding
 * to the write path as well would be useful. For each size, it sets up a TLS server and tests the
 * handshake, transferring the data, and verifying the data is correct after decryption.
 */
public class TLSSocketWriteChannelFragmentationIT extends TLSSocketChannelFragmentation
{
    private ServerSocket server;

    @Before
    public void setup() throws Throwable
    {
        createSSLContext();
        createServer();
    }

    private byte[] blobOfDataSize( int dataBlobSize )
    {
        byte[] blob = new byte[dataBlobSize];
        // If the blob is all zeros, we'd miss data corruption problems in assertions, so
        // fill the data blob with different values.
        for ( int i = 0; i < blob.length; i++ )
        {
            blob[i] = (byte) (i % 128);
        }

        return blob;
    }

    protected void testForBufferSizes( int blobOfDataSize, int networkFrameSize, int userBufferSize ) throws IOException, GeneralSecurityException
    {
        byte[] blob = blobOfDataSize(blobOfDataSize);
        SSLEngine engine = sslCtx.createSSLEngine();
        engine.setUseClientMode( true );
        ByteChannel ch = SocketChannel.open( new InetSocketAddress( server.getInetAddress(), server.getLocalPort() ) );
        ch = new LittleAtATimeChannel( ch, networkFrameSize );

        try ( TLSSocketChannel channel = new TLSSocketChannel( ch, new DevNullLogger(), engine ) )
        {
            ByteBuffer writeBuffer = ByteBuffer.wrap( blob );
            while ( writeBuffer.position() < writeBuffer.capacity() )
            {
                writeBuffer.limit( Math.min( writeBuffer.capacity(), writeBuffer.position() + userBufferSize ) );
                channel.write( writeBuffer );
            }

        }
    }

    protected void createServer() throws IOException
    {
        SSLServerSocketFactory ssf = sslCtx.getServerSocketFactory();
        server = ssf.createServerSocket(0);

        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    //noinspection InfiniteLoopStatement
                    while(true)
                    {
                        Socket client = server.accept();


                        InputStream inputStream = client.getInputStream();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();

                        int read;
                        while ((read = inputStream.read()) != -1)
                        {
                            baos.write( read );
                        }

                        assertThat( blobOfDataSize( baos.size() ), equalTo( baos.toByteArray() ));

                        // client.close(); // TODO: Uncomment this, fix resulting error handling CLOSED event
                    }
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
