/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.connector.socket.ChunkedInput;
import org.neo4j.driver.internal.messaging.AckFailureMessage;
import org.neo4j.driver.internal.messaging.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.FailureMessage;
import org.neo4j.driver.internal.messaging.IgnoredMessage;
import org.neo4j.driver.internal.messaging.InitializeMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1.NoOpRunnable;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.RecordMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.messaging.SuccessMessage;
import org.neo4j.driver.internal.packstream.BufferedChannelInput;

import static org.neo4j.driver.internal.util.BytePrinter.hexStringToBytes;

public class DumpMessage
{
    public static void main( String[] args ) throws IOException
    {
        if( args.length <= 1)
        {
            System.out.println( "Please specify the PackStreamV1 message (without chunk size and 00 00 ending) " +
                                "that you want to unpack in hex strings");
            return;
        }
        StringBuilder hexStr = new StringBuilder();
        for( int i = 0; i < args.length; i ++ )
        {
            hexStr.append( args[i] );
        }

        byte[] bytes = hexStringToBytes( hexStr.toString() );

        // for now we only handle PackStreamV1
        ArrayList<Message> messages = null;
        try
        {
            messages = unpackPackStreamV1WithHeader( bytes );
        }
        catch ( Exception e )
        {
            System.out.println( e.toString() );
            messages = unpackPackStreamV1Message( bytes );
        }

        for ( Message message : messages )
        {
            System.out.println( message );
        }
    }

    public static ArrayList<Message> unpackPackStreamV1WithHeader( byte[] bytes ) throws IOException
    {
        ByteArrayInputStream input = new ByteArrayInputStream( bytes );
        ChunkedInput chunkedInput = new ChunkedInput( Channels.newChannel( input ) );
        PackStreamMessageFormatV1.Reader reader =
                new PackStreamMessageFormatV1.Reader( chunkedInput, chunkedInput.messageBoundaryHook() );
        ArrayList<Message> messages = unpack( reader );
        return messages;
    }

    public static ArrayList<Message> unpackPackStreamV1Message( byte[] bytes ) throws IOException
    {
        ByteArrayInputStream input = new ByteArrayInputStream( bytes );
        BufferedChannelInput bufferedChannelInput = new BufferedChannelInput( 1024*8, Channels.newChannel( input ) );
        PackStreamMessageFormatV1.Reader reader =
                new PackStreamMessageFormatV1.Reader( bufferedChannelInput, new NoOpRunnable() );
        ArrayList<Message> messages = unpack( reader );

        return messages;
    }

    public static ArrayList<Message> unpack( MessageFormat.Reader reader ) throws IOException
    {
        final ArrayList<Message> outcome = new ArrayList<>();
        try
        {
            reader.read( new MessageHandler()
            {
                @Override
                public void handlePullAllMessage()
                {
                    outcome.add( new PullAllMessage() );
                }

                @Override
                public void handleInitializeMessage( String clientNameAndVersion ) throws IOException
                {
                    outcome.add( new InitializeMessage( clientNameAndVersion ) );
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
            } );
        }
        catch ( Neo4jException e )
        {
            throw e;
        }
        return outcome;
    }
}
