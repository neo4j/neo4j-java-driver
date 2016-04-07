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
package org.neo4j.driver.internal.connector.socket;

import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;

import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.util.BytePrinter;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.OK;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests related to the buffer uses in SSLSocketChannel
 */
public class TLSSocketChannelTest
{
    private ByteBuffer plainIn;
    private ByteBuffer cipherIn;
    private ByteBuffer cipherOut;

    private static int bufferSize;
    private static SSLEngine sslEngine;

    @BeforeClass
    public static void setup()
    {
        sslEngine = mock( SSLEngine.class );
        SSLSession session = mock( SSLSession.class );
        when( sslEngine.getSession() ).thenReturn( session );

        // The strategy to enlarge the application buffer: double the size
        doAnswer( new Answer<Integer>()
        {
            @Override
            public Integer answer( InvocationOnMock invocation ) throws Throwable
            {
                bufferSize *= 2;
                if ( bufferSize > 8 )
                {
                    fail( "We do not need a application buffer greater than 8 for all the SSL buffer tests" );
                }
                return bufferSize;
            }
        } ).when( session ).getApplicationBufferSize();

        // The strategy to enlarge the network buffer: double the size
        doAnswer( new Answer<Integer>()
        {
            @Override
            public Integer answer( InvocationOnMock invocation ) throws Throwable
            {
                bufferSize *= 2;
                if ( bufferSize > 8 )
                {
                    fail( "We do not need a network buffer greater than 8 for all the SSL buffer tests" );
                }
                return bufferSize;
            }
        } ).when( session ).getPacketBufferSize();
    }

    @Test
    public void shouldEnlargeApplicationInputBuffer() throws Throwable
    {
        // Given
        bufferSize = 2;
        plainIn = ByteBuffer.allocate( bufferSize );
        ByteBuffer cipherIn = mock( ByteBuffer.class );
        ByteBuffer plainOut = mock( ByteBuffer.class );
        ByteBuffer cipherOut = mock( ByteBuffer.class );

        SocketChannel channel = mock( SocketChannel.class );
        Logger logger = mock( Logger.class );


        TLSSocketChannel sslChannel =
                new TLSSocketChannel( channel, logger, sslEngine, plainIn, cipherIn, plainOut, cipherOut );

        // Write 00 01 02 03 04 05 06 into plainIn, simulating deciphering some bytes
        doAnswer( new Answer<SSLEngineResult>()
        {
            @Override
            public SSLEngineResult answer( InvocationOnMock invocation ) throws Throwable
            {
                Object[] args = invocation.getArguments();
                plainIn = (ByteBuffer) args[1];
                ByteBuffer bytesDeciphered = createBufferWithContent( 6, 0, 6 );

                // Simulating unwrap( cipherIn, plainIn );
                if ( plainIn.remaining() >= bytesDeciphered.capacity() )
                {
                    plainIn.put( bytesDeciphered );
                    return new SSLEngineResult( OK, NOT_HANDSHAKING, 0, 0 );
                }
                else
                {
                    return new SSLEngineResult( BUFFER_OVERFLOW, NOT_HANDSHAKING, 0, 0 );
                }
            }
        } ).when( sslEngine ).unwrap( any( ByteBuffer.class ), any( ByteBuffer.class ) );

        ByteBuffer twoByteBuffer = ByteBuffer.allocate( 2 );
        sslChannel.read( twoByteBuffer );
        sslChannel.read( twoByteBuffer );
        sslChannel.read( twoByteBuffer );

        // Then
        // Should enlarge plainIn buffer to hold all deciphered bytes
        assertEquals( 8, plainIn.capacity() );
        ByteBuffer.allocate( 2 ).flip();
        TestCase.assertEquals( "00 01 ", BytePrinter.hex( twoByteBuffer ) );

        // When trying to read 4 existing bytes and then 6 more bytes
        ByteBuffer buffer = ByteBuffer.allocate( 10 );
        while (buffer.hasRemaining())
        {
            sslChannel.read( buffer );
        }
        // Then
        // Should drain previous deciphered bytes first and then append new bytes after
        assertEquals( 8, plainIn.capacity() );
        buffer.flip();
        assertEquals( "02 03 04 05 00 01 02 03    04 05 ", BytePrinter.hex( buffer ) );
    }

    @Test
    public void shouldEnlargeNetworkInputBuffer() throws Throwable
    {
        // Given
        bufferSize = 2;
        cipherIn = ByteBuffer.allocate( bufferSize );
        plainIn = ByteBuffer.allocate( 1024 );
        ByteBuffer plainOut = mock( ByteBuffer.class );
        ByteBuffer cipherOut = mock( ByteBuffer.class );

        SocketChannel channel = mock( SocketChannel.class );
        Logger logger = mock( Logger.class );

        TLSSocketChannel sslChannel =
                new TLSSocketChannel( channel, logger, sslEngine, plainIn, cipherIn, plainOut, cipherOut );

        final ByteBuffer bytesFromChannel = createBufferWithContent( 6, 0, 6 );

        // Simulating reading from channel and write into cipherIn
        doAnswer( new Answer<Integer>()
        {
            @Override
            public Integer answer( InvocationOnMock invocation ) throws Throwable
            {
                Object[] args = invocation.getArguments();
                cipherIn = (ByteBuffer) args[0];
                return TLSSocketChannel.bufferCopy( bytesFromChannel, cipherIn );
            }
        } ).when( channel ).read( any( ByteBuffer.class ) );

        // Write 00 01 02 03 04 05 06 into plainIn, simulating deciphering some bytes
        doAnswer( new Answer<SSLEngineResult>()
        {
            @Override
            public SSLEngineResult answer( InvocationOnMock invocation ) throws Throwable
            {
                Object[] args = invocation.getArguments();
                plainIn = (ByteBuffer) args[1];

                // Simulating unwrap( cipherIn, plainIn );
                if ( cipherIn.remaining() >= bytesFromChannel.capacity() )
                {
                    ByteBuffer bytesDeciphered = createBufferWithContent( 6, 0, 6 );
                    plainIn.put( bytesDeciphered );
                    cipherIn.position( cipherIn.limit() );
                    return new SSLEngineResult( OK, NOT_HANDSHAKING, 0, 0 );
                }
                else
                {
                    return new SSLEngineResult( BUFFER_UNDERFLOW, NOT_HANDSHAKING, 0, 0 );
                }
            }
        } ).when( sslEngine ).unwrap( any( ByteBuffer.class ), any( ByteBuffer.class ) );


        //When
        sslChannel.read( ByteBuffer.allocate( 2 ) );
        sslChannel.read( ByteBuffer.allocate( 2 ) );
        sslChannel.read( ByteBuffer.allocate( 2 ) );

        // Then
        assertEquals( 8, cipherIn.capacity() );
        assertEquals( "00 01 02 03 04 05 00 00    ", BytePrinter.hex( cipherIn ) );
    }

    @Test
    public void shouldCompactNetworkInputBufferBeforeReadingMoreFromChannel() throws Throwable
    {
        // Given
        bufferSize = 8;
        cipherIn = ByteBuffer.allocate( bufferSize );
        plainIn = ByteBuffer.allocate( 1024 );
        ByteBuffer plainOut = mock( ByteBuffer.class );
        ByteBuffer cipherOut = mock( ByteBuffer.class );

        SocketChannel channel = mock( SocketChannel.class );
        Logger logger = mock( Logger.class );

        TLSSocketChannel sslChannel =
                new TLSSocketChannel( channel, logger, sslEngine, plainIn, cipherIn, plainOut, cipherOut );


        // Simulate reading from channel and write into cipherIn
        doAnswer( new Answer<Integer>()
        {
            @Override
            public Integer answer( InvocationOnMock invocation ) throws Throwable
            {
                Object[] args = invocation.getArguments();
                cipherIn = (ByteBuffer) args[0];
                ByteBuffer bytesFromChannel = createBufferWithContent( 4, 0, 4 );   // write 00 01 02 03 into cipherIn
                TLSSocketChannel.bufferCopy( bytesFromChannel, cipherIn );
                return cipherIn.position();
            }
        } ).when( channel ).read( any( ByteBuffer.class ) );


        final int[] rounds = {0}; // A counter for how many times we've entered unwrap method
        // Simulating deciphering some bytes.
        // Unfortunately we cannot simply treat unwrap as a black box this time
        doAnswer( new Answer<SSLEngineResult>()
        {
            @Override
            public SSLEngineResult answer( InvocationOnMock invocation ) throws Throwable
            {
                rounds[0]++;
                Object[] args = invocation.getArguments();
                cipherIn = (ByteBuffer) args[0];
                plainIn = (ByteBuffer) args[1];

                switch ( rounds[0] )
                {
                case 1:
                    // 00 01 02 03 -> [XX XX] 02 03
                    cipherIn.position( 2 );                 // consume the first 2 bytes and re-enter unwrap
                    return new SSLEngineResult( OK, NOT_HANDSHAKING, 0, 0 );
                case 2:
                    // [XX XX] 02 03 -> 02 03
                    bufferSize = bufferSize / 2;            // so that we could value the same size back
                    return new SSLEngineResult( BUFFER_UNDERFLOW, NOT_HANDSHAKING, 0, 0 ); // waiting for more data
                case 3:
                    // 02 03 00 01 02 03 -> [XX XX XX XX XX XX]
                    ByteBuffer bytesDeciphered = createBufferWithContent( 2, 0, 2 );
                    plainIn.put( bytesDeciphered );
                    cipherIn.position( cipherIn.limit() );  // consume all bytes in cipherIn to exit unwrap
                    return new SSLEngineResult( OK, NOT_HANDSHAKING, 0, 0 );
                default:
                    fail( "Should not call unwrap after all the bytes in cipherIn already consumed and OK returned" );
                    return null;
                }

            }
        } ).when( sslEngine ).unwrap( any( ByteBuffer.class ), any( ByteBuffer.class ) );

        //When
        sslChannel.read( ByteBuffer.allocate( 8 ) );
        sslChannel.read( ByteBuffer.allocate( 8 ) );

        // Then
        assertEquals( 8, cipherIn.capacity() );
        assertEquals( "02 03 00 01 02 03 00 00    ", BytePrinter.hex( cipherIn ) );
    }

    @Test
    public void shouldEnlargeNetworkOutputBuffer() throws Throwable
    {
        // Given
        bufferSize = 2;
        ByteBuffer cipherIn = mock( ByteBuffer.class );
        ByteBuffer plainIn = mock( ByteBuffer.class );
        ByteBuffer plainOut = mock( ByteBuffer.class );
        cipherOut = ByteBuffer.allocate( bufferSize );

        final ByteBuffer buffer = ByteBuffer.allocate( 2 );

        SocketChannel channel = mock( SocketChannel.class );
        Logger logger = mock( Logger.class );

        TLSSocketChannel sslChannel =
                new TLSSocketChannel( channel, logger, sslEngine, plainIn, cipherIn, plainOut, cipherOut );


        // Simulating encrypting some bytes
        doAnswer( new Answer<SSLEngineResult>()
        {
            @Override
            public SSLEngineResult answer( InvocationOnMock invocation ) throws Throwable
            {
                Object[] args = invocation.getArguments();
                cipherOut = (ByteBuffer) args[1];

                // Simulating wrap( buffer, cipherIn );
                ByteBuffer bytesToChannel = createBufferWithContent( 6, 0, 6 );
                if ( cipherOut.remaining() >= bytesToChannel.capacity() )
                {
                    buffer.position( buffer.limit() );
                    cipherOut.put( bytesToChannel );
                    return new SSLEngineResult( OK, NOT_HANDSHAKING, 0, 0 );
                }
                else
                {
                    return new SSLEngineResult( BUFFER_OVERFLOW, NOT_HANDSHAKING, 0, 0 );
                }

            }
        } ).when( sslEngine ).wrap( any( ByteBuffer.class ), any( ByteBuffer.class ) );

        //When
        sslChannel.write( buffer );

        // Then
        assertEquals( 8, cipherOut.capacity() );
        assertEquals( "00 01 02 03 04 05 00 00    ", BytePrinter.hex( cipherOut ) );
    }

    private static ByteBuffer createBufferWithContent( int size, int contentStartPos, int contentLength )
    {
        ByteBuffer buffer = ByteBuffer.allocate( size );

        buffer.position( contentStartPos );
        buffer.limit( contentLength + contentStartPos );

        for ( int i = 0; i < contentLength; i++ )
        {
            buffer.put( i + contentStartPos, (byte) i );
        }

        return buffer;
    }
}
