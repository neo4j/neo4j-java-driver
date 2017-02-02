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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;

import org.neo4j.driver.internal.net.ChunkedOutput;
import org.neo4j.driver.packstream.PackStream;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.GraphHydrant;
import org.neo4j.driver.v1.types.SelfContainedNode;
import org.neo4j.driver.v1.types.SelfContainedPath;
import org.neo4j.driver.v1.types.SelfContainedRelationship;
import org.neo4j.driver.v1.util.DumpMessage;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.neo4j.driver.v1.Values.EmptyMap;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.Values.value;

public class MessageFormatTest
{
    public MessageFormat format = new PackStreamMessageFormatV1();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldPackAllRequests() throws Throwable
    {
        assertSerializes( new RunMessage( "Hello", parameters().asMap( ofValue())) );
        assertSerializes( new RunMessage( "Hello", parameters( "a", 12 ).asMap( ofValue()) ) );
        assertSerializes( new PullAllMessage() );
        assertSerializes( new DiscardAllMessage() );
        assertSerializes( new IgnoredMessage() );
        assertSerializes( new FailureMessage( "Neo.Banana.Bork.Birk", "Hello, world!" ) );
        assertSerializes( new ResetMessage() );
        assertSerializes( new InitMessage( "JavaDriver/1.0.0", parameters().asMap( ofValue()) ) );
    }

    @Test
    public void shouldUnpackAllResponses() throws Throwable
    {
        assertSerializes( new RecordMessage( new Value[]{value( 1337L )} ) );
        //assertSerializes( new SuccessMessage( new HashMap<String,Value>() ) );
    }

    @Test
    public void shouldUnpackAllValues() throws Throwable
    {
        assertSerializesValue( value( parameters( "cat", null, "dog", null ) ) );
        assertSerializesValue( value( parameters( "k", 12, "a", "banana" ) ) );
        assertSerializesValue( value( asList( "k", 12, "a", "banana" ) ) );
        assertSerializesValue( value(
                new SelfContainedNode( 1, Collections.singletonList( "User" ), parameters( "name", "Bob", "age", 45 ).asMap(
                        ofValue()) )
        ) );
        assertSerializesValue( value( new SelfContainedNode( 1 ) ) );
        assertSerializesValue( value(
                new SelfContainedRelationship( 1, 1, 1,
                        "KNOWS",
                        parameters( "name", "Bob", "age", 45 ).asMap( ofValue()) ) ) );
        assertSerializesValue( value(
                new SelfContainedPath(
                        new SelfContainedNode( 1 ),
                        new SelfContainedRelationship( 2, 1, 3,
                                "KNOWS", EmptyMap.asMap( ofValue()) ),
                        new SelfContainedNode( 3 ),
                        new SelfContainedRelationship( 4, 3, 5,
                                "LIKES", EmptyMap.asMap( ofValue()) ),
                        new SelfContainedNode( 5 )
                ) ) );
        assertSerializesValue( value( new SelfContainedPath( new SelfContainedNode( 1 ) ) ) );
    }

    @Test
    public void shouldGiveHelpfulErrorOnMalformedNodeStruct() throws Throwable
    {
        // Given
        ByteArrayOutputStream out = new ByteArrayOutputStream( 128 );
        WritableByteChannel writable = Channels.newChannel( out );
        PackStream packer = new PackStream( new ChunkedOutput( writable ) );

        packer.packStructHeader( 1, PackStreamMessageFormatV1.MSG_RECORD );
        packer.packListHeader( 1 );
        packer.packStructHeader( 0, GraphHydrant.NODE );
        packer.flush();

        // Expect
        exception.expect( IOException.class );
        exception.expectMessage( startsWith( "Invalid structure size 0" ) );

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
        ArrayList<Message> unpackedMessages = unpack( format, out.toByteArray() );
        assertThat( unpackedMessages.toString(), equalTo( asList( messages ).toString() ) );
    }

    private ArrayList<Message> unpack( MessageFormat format, byte[] bytes ) throws IOException
    {
        ByteArrayInputStream input = new ByteArrayInputStream( bytes );
        MessageFormat.Reader reader = format.newReader( Channels.newChannel( input ) );
        ArrayList<Message> messages = new ArrayList<>();
        DumpMessage.unpack( messages, reader );
        return messages;
    }

}
