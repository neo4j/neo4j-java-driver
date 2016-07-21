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
package org.neo4j.driver.internal.net;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class BufferingChunkedInputFuzzTest
{

    @Test
    public void shouldHandleAllMessageBoundaries() throws IOException
    {
        byte[] expected = new byte[256];
        for ( int i = 0; i < 256; i++ )
        {
            expected[i] = (byte) (Byte.MIN_VALUE + i);
        }

        for ( int i = 0; i < 256; i++ )
        {
            BufferingChunkedInput input = new BufferingChunkedInput( splitChannel( expected, i ) );
            byte[] dst = new byte[256];
            input.readBytes( dst, 0, dst.length );

            assertThat( dst, equalTo( expected ) );
        }
    }

    @Test
    public void messageSizeFuzzTest() throws IOException
    {
        int maxSize = 1 << 16; // 0x10000
        Random random = new Random();
        for ( int i = 0; i < 1000; i++)
        {
            int size = random.nextInt( maxSize - 1 ) + 1; //[0, 0xFFFF - 1] + 1 = [1, 0xFFFF]
            byte[] expected = new byte[size];
            Arrays.fill(expected, (byte)42);
            BufferingChunkedInput input = new BufferingChunkedInput( channel( expected, 0, size ) );

            byte[] dst = new byte[size];
            input.readBytes( dst, 0, size);

            assertThat( dst, equalTo( expected ) );
        }
    }

    ReadableByteChannel splitChannel( byte[] bytes, int split )
    {
        assert split >= 0 && split < bytes.length;
        assert split <= Short.MAX_VALUE;
        assert bytes.length <= Short.MAX_VALUE;

        return packets( channel( bytes, 0, split ), channel( bytes, split, bytes.length ) );
    }

    ReadableByteChannel channel( byte[] bytes, int from, int to )
    {
        int size = to - from;
        ByteBuffer packet = ByteBuffer.allocate( 4 + size );
        packet.put( (byte) ((size >> 8) & 0xFF) );
        packet.put( (byte) (size & 0xFF) );
        for ( int i = from; i < to; i++ )
        {
            packet.put( bytes[i] );
        }
        packet.put( (byte) 0 );
        packet.put( (byte) 0 );
        packet.flip();

        return asChannel( packet );
    }

    private ReadableByteChannel packets( final ReadableByteChannel... channels )
    {

        return new ReadableByteChannel()
        {
            private int index = 0;

            @Override
            public int read( ByteBuffer dst ) throws IOException
            {
                return channels[index++].read( dst );
            }

            @Override
            public boolean isOpen()
            {
                return false;
            }

            @Override
            public void close() throws IOException
            {

            }
        };
    }

    private ReadableByteChannel asChannel( final ByteBuffer buffer )
    {
        return new ReadableByteChannel()
        {
            @Override
            public int read( ByteBuffer dst ) throws IOException
            {
                int len = Math.min( dst.remaining(), buffer.remaining() );
                for ( int i = 0; i < len; i++ )
                {
                    dst.put( buffer.get() );
                }
                return len;

            }

            @Override
            public boolean isOpen()
            {
                return true;
            }

            @Override
            public void close() throws IOException
            {

            }
        };
    }
}
