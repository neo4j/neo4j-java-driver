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
package org.neo4j.driver.internal.messaging;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.SimpleNode;
import org.neo4j.driver.internal.SimplePath;
import org.neo4j.driver.internal.SimpleRelationship;
import org.neo4j.driver.internal.connector.socket.ChunkedOutput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.util.DumpMessage;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.neo4j.driver.Values.properties;
import static org.neo4j.driver.Values.value;

public class MessageFormatTest
{
    public MessageFormat format = new PackStreamMessageFormatV1();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldPackAllRequests() throws Throwable
    {
        assertSerializes( new RunMessage( "Hello", properties() ) );
        assertSerializes( new RunMessage( "Hello", properties( "a", 12 ) ) );
        assertSerializes( new PullAllMessage() );
        assertSerializes( new DiscardAllMessage() );
        assertSerializes( new IgnoredMessage() );
        assertSerializes( new FailureMessage( "Neo.Banana.Bork.Birk", "Hello, world!" ) );
        assertSerializes( new InitializeMessage( "JavaDriver/1.0.0" ) );
    }

    @Test
    public void shouldUnpackAllResponses() throws Throwable
    {
        assertSerializes( new RecordMessage( new Value[]{value( 1337l )} ) );
        assertSerializes( new SuccessMessage( new HashMap<String,Value>() ) );
    }

    @Test
    public void shouldUnpackAllValues() throws Throwable
    {
        assertSerializesValue( value( properties( "cat", null, "dog", null ) ) );
        assertSerializesValue( value( properties( "k", 12, "a", "banana" ) ) );
        assertSerializesValue( value( asList( "k", 12, "a", "banana" ) ) );
        assertSerializesValue( value(
                new SimpleNode( "node/1", asList( "User" ), properties( "name", "Bob", "age", 45 ) ) ) );
        assertSerializesValue( value( new SimpleNode( "node/1" ) ) );
        assertSerializesValue( value(
                new SimpleRelationship( "rel/1", "node/1", "node/1",
                        "KNOWS",
                        properties( "name", "Bob", "age", 45 ) ) ) );
        assertSerializesValue( value(
                new SimplePath(
                        new SimpleNode( "node/1" ),
                        new SimpleRelationship( "relationship/1", "node/1", "node/1",
                                "KNOWS", properties() ),
                        new SimpleNode( "node/1" ),
                        new SimpleRelationship( "relationship/2", "node/1", "node/1",
                                "LIKES", properties() ),
                        new SimpleNode( "node/1" )
                ) ) );
        assertSerializesValue( value( new SimplePath( new SimpleNode( "node/1" ) ) ) );
    }

    @Test
    public void shouldGiveHelpfulErrorOnMalformedNodeStruct() throws Throwable
    {
        // Given
        ByteArrayOutputStream out = new ByteArrayOutputStream( 128 );
        WritableByteChannel writable = Channels.newChannel( out );
        PackStream.Packer packer = new PackStream.Packer( new ChunkedOutput( writable ) );

        packer.packStructHeader( 1, PackStreamMessageFormatV1.MSG_RECORD );
        packer.packListHeader( 1 );
        packer.packStructHeader( 0, PackStreamMessageFormatV1.NODE );
        packer.flush();

        // Expect
        exception.expect( RuntimeException.class );
        exception.expectMessage( startsWith(
                "Failed to unpack value: Invalid message received, serialized NODE structures should have 3 fields, " +
                "received NODE structure has 0 fields." ) );

        // When
        unpack( format, out.toByteArray() );
    }

    private void assertSerializesValue( Value value ) throws IOException
    {
        assertSerializes( new RecordMessage( new Value[]{value} ) );
    }

    private void assertSerializes( Message... messages ) throws IOException
    {
        // Pack
        final ByteArrayOutputStream out = new ByteArrayOutputStream( 128 );
        MessageFormat.Writer writer = format.newWriter( Channels.newChannel( out ) );
        for ( Message message : messages )
        {
            writer.write( message );
        }
        writer.flush();

        // Unpack
        assertThat( unpack( format, out.toByteArray() ), equalTo( asList( messages ) ) );
    }

    private ArrayList<Message> unpack( MessageFormat format, byte[] bytes ) throws IOException
    {
        try
        {
            ByteArrayInputStream input = new ByteArrayInputStream( bytes );
            MessageFormat.Reader reader = format.newReader( Channels.newChannel( input ) );
            ArrayList<Message> messages = new ArrayList<>();
            DumpMessage.unpack( messages, reader );
            return messages;
        }
        catch( Exception e )
        {
            throw new RuntimeException(
                    String.format( "Failed to unpack value: %s Raw data:\n%s",
                            e.getMessage(), BytePrinter.hex( bytes ) ), e );
        }
    }

}
