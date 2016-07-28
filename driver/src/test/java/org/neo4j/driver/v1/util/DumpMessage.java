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
package org.neo4j.driver.v1.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.net.ChunkedInput;
import org.neo4j.driver.internal.messaging.ResetMessage;
import org.neo4j.driver.internal.messaging.AckFailureMessage;
import org.neo4j.driver.internal.messaging.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.FailureMessage;
import org.neo4j.driver.internal.messaging.IgnoredMessage;
import org.neo4j.driver.internal.messaging.InitMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.RecordMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.messaging.SuccessMessage;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.v1.Value;

public class DumpMessage
{
    public static void main( String[] args )
    {
        if ( args.length < 1 )
        {
            System.out.println( "Please specify PackStreamV1 messages " +
                                "(or PackStreamV1 messages in chunks) " +
                                "that you want to unpack in hex strings. " );
            return;
        }
        StringBuilder hexStr = new StringBuilder();
        for ( String arg : args )
        {
            hexStr.append( arg );
        }

        byte[] bytes = BytePrinter.hexStringToBytes( hexStr.toString() );

        // for now we only handle PackStreamV1
        ArrayList<Message> messages;
        try
        {
            // first try to interpret as a message with chunk header and 00 00 ending
            messages = unpackPackStreamV1WithHeader( bytes );
        }
        catch ( IOException e )
        {
            // fall back to interpret as a message without chunk header and 00 00 ending
            try
            {
                messages = unpackPackStreamV1Message( bytes );
            }
            catch ( IOException ee )
            {
                // If both of them failed, then print the debug info for both of them.
                System.err.println( "Failed to interpret the given hex string." );
                e.printStackTrace();

                ee.printStackTrace();
                return;
            }
        }

        for ( Message message : messages )
        {
            System.out.println( message );
        }
    }

    public static ArrayList<Message> unpackPackStreamV1WithHeader( byte[] bytes ) throws IOException
    {
        ArrayList<Message> messages = new ArrayList<>();
        ByteArrayChunkedInput chunkedInput = new ByteArrayChunkedInput( bytes );
        try
        {
            PackStreamMessageFormatV1.Reader reader =
                    new PackStreamMessageFormatV1.Reader( chunkedInput, chunkedInput.messageBoundaryHook() );
            unpack( messages, reader );
            return messages;
        }
        catch ( Exception e )
        {
            int offset = chunkedInput.prePos;
            throw new IOException(
                    "Error when interpreting the message as PackStreamV1 message with chunk size and 00 00 ending:" +
                    "\nMessage interpreted : " + messages +
                    "\n" + BytePrinter.hexInOneLine( ByteBuffer.wrap( bytes ), 0, bytes.length ) + /* all bytes */
                    "\n" + padLeft( offset ) /* the indicator of the error place*/, e );
        }
    }

    public static ArrayList<Message> unpackPackStreamV1Message( byte[] bytes ) throws IOException
    {
        ArrayList<Message> messages = new ArrayList<>();
        byte[] bytesWithHeadTail = putBytesInOneChunk( bytes );
        ByteArrayChunkedInput chunkedInput = new ByteArrayChunkedInput( bytesWithHeadTail );

        try
        {
            PackStreamMessageFormatV1.Reader reader =
                    new PackStreamMessageFormatV1.Reader( chunkedInput, chunkedInput.messageBoundaryHook() );
            unpack( messages, reader );
            return messages;
        }
        catch ( Exception e )
        {
            int offset = chunkedInput.prePos - 2; // not including the chunk size
            throw new IOException(
                    "Error when interpreting the message as PackStream message:" +
                    "\nMessage interpreted : " + messages +
                    "\n" + BytePrinter.hexInOneLine( ByteBuffer.wrap( bytes ), 0, bytes.length ) + /* all bytes */
                    "\n" + padLeft( offset ) /* the indicator of the error place*/, e );
        }
    }

    private static String padLeft( int offset )
    {
        StringBuilder output = new StringBuilder();
        for ( int i = 0; i < offset; i++ )
        {
            output.append( "  " );
            if ( (i + 1) % 8 == 0 )
            {
                output.append( "    " );
            }
            else
            {
                output.append( " " );
            }
        }
        output.append( "^" );
        return output.toString();
    }

    private static byte[] putBytesInOneChunk( byte[] bytes )
    {
        byte[] bytesWithHeadAndTail = new byte[bytes.length + 2 + 2]; // 2 for head and 2 for tail
        bytesWithHeadAndTail[0] = (byte) (bytes.length >>> 8);
        bytesWithHeadAndTail[1] = (byte) bytes.length;
        System.arraycopy( bytes, 0, bytesWithHeadAndTail, 2, bytes.length );
        return bytesWithHeadAndTail;
    }

    public static List<Message> unpack( List<Message> outcome, MessageFormat.Reader reader )
            throws IOException
    {
        do
        {
            reader.read( new MessageRecordedMessageHandler( outcome ) );
        }
        while ( reader.hasNext() );
        return outcome;
    }

    /**
     *  This class modified {@link ChunkedInput} to accept a byte array as input and keep track of how many bytes we
     *  have read from the input byte array.
     */
    private static class ByteArrayChunkedInput implements PackInput
    {
        private int prePos;
        private int curPos;
        private final int size;

        private final ChunkedInput delegate;

        public ByteArrayChunkedInput( byte[] bytes )
        {
            prePos = curPos = 0;
            size = bytes.length;
            ByteArrayInputStream input = new ByteArrayInputStream( bytes );
            ReadableByteChannel ch = Channels.newChannel( input );
            this.delegate = new ChunkedInput( ch )
            {
                @Override
                protected int readChunkSize() throws IOException
                {
                    prePos = curPos;
                    int chunkSize = super.readChunkSize();
                    curPos += 2;
                    return chunkSize;
                }
            };
        }

        @Override
        public boolean hasMoreData() throws IOException
        {
            return curPos < size;
        }

        @Override
        public byte readByte()
        {
            prePos = curPos;
            byte read = delegate.readByte();
            curPos += 1;
            return read;
        }

        @Override
        public short readShort()
        {
            prePos = curPos;
            short read = delegate.readShort();
            curPos += 2;
            return read;
        }

        @Override
        public int readInt()
        {
            prePos = curPos;
            int read = delegate.readInt();
            curPos += 4;
            return read;
        }

        @Override
        public long readLong()
        {
            prePos = curPos;
            long read = delegate.readLong();
            curPos += 8;
            return read;
        }

        @Override
        public double readDouble()
        {
            prePos = curPos;
            double read = delegate.readDouble();
            curPos += 8;
            return read;
        }

        @Override
        public PackInput readBytes( byte[] into, int offset, int toRead )
        {
            prePos = curPos;
            PackInput packInput = delegate.readBytes( into, offset, toRead );
            curPos += toRead;
            return packInput;
        }

        @Override
        public byte peekByte()
        {
            return delegate.peekByte();
        }

        public Runnable messageBoundaryHook()
        {
            // the method will call readChunkSize method so no need to +2
            final Runnable runnable = delegate.messageBoundaryHook();
            return new Runnable()
            {
                @Override
                public void run()
                {
                    prePos = curPos;
                    runnable.run();
                }
            };
        }

    }

    /**
     * All the interpreted messages will be appended to the input message array even if an error happens when
     * decoding other messages latter.
     */
    private static class MessageRecordedMessageHandler implements MessageHandler
    {
        private final List<Message> outcome;

        public MessageRecordedMessageHandler( List<Message> outcome )
        {
            this.outcome = outcome;
        }

        @Override
        public void handlePullAllMessage()
        {
            outcome.add( new PullAllMessage() );
        }

        @Override
        public void handleInitMessage( String clientNameAndVersion, Map<String,Value> authToken ) throws IOException
        {
            outcome.add( new InitMessage( clientNameAndVersion, authToken ) );
        }

        @Override
        public void handleRunMessage( String statement, Map<String,Value> parameters )
        {
            outcome.add( new RunMessage( statement, parameters ) );
        }

        @Override
        public void handleDiscardAllMessage()
        {
            outcome.add( new DiscardAllMessage() );
        }

        @Override
        public void handleResetMessage()
        {
            outcome.add( new ResetMessage() );
        }

        @Override
        public void handleAckFailureMessage()
        {
            outcome.add( new AckFailureMessage() );
        }

        @Override
        public void handleSuccessMessage( Map<String,Value> meta )
        {
            outcome.add( new SuccessMessage( meta ) );
        }

        @Override
        public void handleRecordMessage( Value[] fields )
        {
            outcome.add( new RecordMessage( fields ) );
        }

        @Override
        public void handleFailureMessage( String code, String message )
        {
            outcome.add( new FailureMessage( code, message ) );
        }

        @Override
        public void handleIgnoredMessage()
        {
            outcome.add( new IgnoredMessage() );
        }
    }
}
