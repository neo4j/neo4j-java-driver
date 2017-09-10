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
package org.neo4j.driver.internal.messaging;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;

import org.neo4j.driver.internal.net.BufferingChunkedInput;
import org.neo4j.driver.internal.net.ChunkedOutput;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.DumpMessage;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * This tests network fragmentation of messages. Given a set of messages, it will serialize and chunk the message up
 * to a specified chunk size. Then it will split that data into a specified number of fragments, trying every possible
 * permutation of fragment sizes for the specified number. For instance, assuming an unfragmented message size of 15,
 * and a fragment count of 3, it will create fragment size permutations like:
 * <p>
 * [1,1,13]
 * [1,2,12]
 * [1,3,11]
 * ..
 * [12,1,1]
 * <p>
 * For each permutation, it delivers the fragments to the protocol implementation, and asserts the protocol handled
 * them properly.
 */
public class FragmentedMessageDeliveryTest
{
    private final MessageFormat format = new PackStreamMessageFormatV1();

    // Only test one chunk size for now, this can be parameterized to test lots of different ones
    private int chunkSize = 16;

    // Only test one message for now. This can be parameterized later to test lots of different ones
    private Message[] messages = new Message[]{ new RunMessage( "Mjölnir", Collections.<String, Value>emptyMap() )};

    @Test
    public void testFragmentedMessageDelivery() throws Throwable
    {
        // Given
        byte[] unfragmented = serialize( messages );

        // When & Then
        int n = unfragmented.length;
        for ( int i = 1; i < n - 1; i++ )
        {
            for ( int j = 1; j < n - i; j++ )
            {
                testPermutation( unfragmented, i, j, n - i - j );
            }
        }
    }

    private void testPermutation( byte[] unfragmented, int... sizes ) throws IOException
    {
        int pos = 0;
        ByteBuffer[] fragments = new ByteBuffer[sizes.length];
        for ( int i = 0; i < sizes.length; i++ )
        {
            fragments[i] = ByteBuffer.wrap( unfragmented, pos, sizes[i] );
            pos += sizes[i];
        }
        testPermutation( unfragmented, fragments );
    }

    private void testPermutation( byte[] unfragmented, ByteBuffer[] fragments ) throws IOException
    {

        // When data arrives split up according to the current permutation
        ReadableByteChannel[] channels = new ReadableByteChannel[fragments.length];
        for ( int i = 0; i < fragments.length; i++ )
        {
            channels[i] = packet( fragments[i] );
        }

        ReadableByteChannel fragmentedChannel = packets( channels );
        BufferingChunkedInput input = new BufferingChunkedInput( fragmentedChannel );
        MessageFormat.Reader reader = format.newReader( input );

        ArrayList<Message> packedMessages = new ArrayList<>();
        DumpMessage.unpack( packedMessages, reader );

        assertThat( packedMessages, equalTo(asList(messages)) );
    }

    private ReadableByteChannel packet( ByteBuffer buffer )
    {
        //NOTE buffer.array is ok here since we know buffer is backed by array
        return Channels.newChannel(
                new ByteArrayInputStream( buffer.array() ) );
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

    private byte[] serialize( Message... msgs ) throws IOException
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream( 128 );

        ChunkedOutput output = new ChunkedOutput( chunkSize + 2 /* for chunk header */, Channels.newChannel( out ) );
        PackStreamMessageFormatV1.Writer writer =
                new PackStreamMessageFormatV1.Writer( output, output.messageBoundaryHook(), true );
        for ( Message message : messages )
        {
            writer.write( message );
        }
        writer.flush();

        return out.toByteArray();
    }
}
