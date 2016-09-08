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

import java.io.IOException;
import java.io.OutputStream;
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
 * - Read buffer size
 *
 * It tests every possible combination, and it does this currently only for the read path, expanding
 * to the write path as well would be useful. For each size, it sets up a TLS server and tests the
 * handshake, transferring the data, and verifying the data is correct after decryption.
 */
public class TLSSocketChannelReadFragmentationIT extends TLSSocketChannelFragmentation
{
    private byte[] blobOfData;
    private ServerSocket server;



    private void blobOfDataSize( int dataBlobSize )
    {
        blobOfData = new byte[dataBlobSize];
        // If the blob is all zeros, we'd miss data corruption problems in assertions, so
        // fill the data blob with different values.
        for ( int i = 0; i < blobOfData.length; i++ )
        {
            blobOfData[i] = (byte) (i % 128);
        }
    }

    protected void testForBufferSizes( int blobOfDataSize, int networkFrameSize, int userBufferSize ) throws IOException, GeneralSecurityException
    {
        blobOfDataSize(blobOfDataSize);
        SSLEngine engine = sslCtx.createSSLEngine();
        engine.setUseClientMode( true );
        ByteChannel ch = SocketChannel.open( new InetSocketAddress( server.getInetAddress(), server.getLocalPort() ) );
        ch = new LittleAtATimeChannel( ch, networkFrameSize );

        try ( TLSSocketChannel channel = new TLSSocketChannel( ch, new DevNullLogger(), engine ) )
        {
            ByteBuffer readBuffer = ByteBuffer.allocate( blobOfData.length );
            while ( readBuffer.position() < readBuffer.capacity() )
            {
                readBuffer.limit( Math.min( readBuffer.capacity(), readBuffer.position() + userBufferSize ) );
                channel.read( readBuffer );
            }

            assertThat( readBuffer.array(), equalTo( blobOfData ) );
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
                        OutputStream outputStream = client.getOutputStream();
                        outputStream.write( blobOfData );
                        outputStream.flush();
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
