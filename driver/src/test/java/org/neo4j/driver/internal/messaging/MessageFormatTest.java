/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.async.BoltProtocolUtil;
import org.neo4j.driver.internal.async.ChannelPipelineBuilderImpl;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.outbound.ChunkAwareByteBufOutput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
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
        assertSerializes( new RunMessage( "Hello", parameters().asMap( ofValue() ) ) );
        assertSerializes( new RunMessage( "Hello", parameters( "a", 12 ).asMap( ofValue() ) ) );
        assertSerializes( new PullAllMessage() );
        assertSerializes( new DiscardAllMessage() );
        assertSerializes( new IgnoredMessage() );
        assertSerializes( new FailureMessage( "Neo.Banana.Bork.Birk", "Hello, world!" ) );
        assertSerializes( new ResetMessage() );
        assertSerializes( new InitMessage( "JavaDriver/1.0.0", parameters().asMap( ofValue() ) ) );
    }

    @Test
    public void shouldUnpackAllResponses() throws Throwable
    {
        assertSerializes( new RecordMessage( new Value[]{value( 1337L )} ) );
        assertSerializes( new SuccessMessage( new HashMap<>() ) );
    }

    @Test
    public void shouldUnpackAllValues() throws Throwable
    {
        assertSerializesValue( value( parameters( "cat", null, "dog", null ) ) );
        assertSerializesValue( value( parameters( "k", 12, "a", "banana" ) ) );
        assertSerializesValue( value( asList( "k", 12, "a", "banana" ) ) );
        assertSerializesValue( value(
                new InternalNode( 1, Collections.singletonList( "User" ), parameters( "name", "Bob", "age", 45 ).asMap(
                        ofValue() ) )
        ) );
        assertSerializesValue( value( new InternalNode( 1 ) ) );
        assertSerializesValue( value(
                new InternalRelationship( 1, 1, 1,
                        "KNOWS",
                        parameters( "name", "Bob", "age", 45 ).asMap( ofValue() ) ) ) );
        assertSerializesValue( value(
                new InternalPath(
                        new InternalNode( 1 ),
                        new InternalRelationship( 2, 1, 3,
                                "KNOWS", EmptyMap.asMap( ofValue() ) ),
                        new InternalNode( 3 ),
                        new InternalRelationship( 4, 3, 5,
                                "LIKES", EmptyMap.asMap( ofValue() ) ),
                        new InternalNode( 5 )
                ) ) );
        assertSerializesValue( value( new InternalPath( new InternalNode( 1 ) ) ) );
    }

    @Test
    public void shouldGiveHelpfulErrorOnMalformedNodeStruct() throws Throwable
    {
        // Given
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput();
        ByteBuf buf = Unpooled.buffer();
        output.start( buf );
        PackStream.Packer packer = new PackStream.Packer( output );

        packer.packStructHeader( 1, PackStreamMessageFormatV1.MSG_RECORD );
        packer.packListHeader( 1 );
        packer.packStructHeader( 0, PackStreamMessageFormatV1.NODE );

        output.stop();
        BoltProtocolUtil.writeMessageBoundary( buf );

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( startsWith(
                "Invalid message received, serialized NODE structures should have 3 fields, " +
                "received NODE structure has 0 fields." ) );

        // When
        unpack( buf, newEmbeddedChannel() );
    }

    private void assertSerializesValue( Value value ) throws Throwable
    {
        assertSerializes( new RecordMessage( new Value[]{value} ) );
    }

    private void assertSerializes( Message message ) throws Throwable
    {
        EmbeddedChannel channel = newEmbeddedChannel();

        ByteBuf packed = pack( message, channel );
        Message unpackedMessage = unpack( packed, channel );

        assertEquals( message, unpackedMessage );
    }

    private EmbeddedChannel newEmbeddedChannel()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        setMessageDispatcher( channel, new MemorizingInboundMessageDispatcher( channel, DEV_NULL_LOGGING ) );
        new ChannelPipelineBuilderImpl().build( format, channel.pipeline(), DEV_NULL_LOGGING );
        return channel;
    }

    private ByteBuf pack( Message message, EmbeddedChannel channel )
    {
        assertTrue( channel.writeOutbound( message ) );

        ByteBuf[] packedMessages = channel.outboundMessages()
                .stream()
                .map( msg -> (ByteBuf) msg )
                .toArray( ByteBuf[]::new );

        return Unpooled.wrappedBuffer( packedMessages );
    }

    private Message unpack( ByteBuf packed, EmbeddedChannel channel ) throws Throwable
    {
        channel.writeInbound( packed );

        InboundMessageDispatcher dispatcher = messageDispatcher( channel );
        MemorizingInboundMessageDispatcher memorizingDispatcher = ((MemorizingInboundMessageDispatcher) dispatcher);

        Throwable error = memorizingDispatcher.currentError();
        if ( error != null )
        {
            throw error;
        }

        List<Message> unpackedMessages = memorizingDispatcher.messages();

        assertEquals( 1, unpackedMessages.size() );
        return unpackedMessages.get( 0 );
    }
}
