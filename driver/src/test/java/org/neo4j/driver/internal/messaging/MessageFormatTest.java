/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
import io.netty.handler.codec.EncoderException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.async.connection.BoltProtocolUtil;
import org.neo4j.driver.internal.async.connection.ChannelPipelineBuilderImpl;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.outbound.ChunkAwareByteBufOutput;
import org.neo4j.driver.internal.messaging.common.CommonValueUnpacker;
import org.neo4j.driver.internal.messaging.request.InitMessage;
import org.neo4j.driver.internal.messaging.response.FailureMessage;
import org.neo4j.driver.internal.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.util.messaging.KnowledgeableMessageFormat;
import org.neo4j.driver.internal.util.messaging.MemorizingInboundMessageDispatcher;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyPathValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledRelationshipValue;

class MessageFormatTest
{
    public MessageFormat format = new MessageFormatV3();

    @Test
    void shouldUnpackAllResponses() throws Throwable
    {
        assertSerializes( new FailureMessage( "Hello", "World!" ) );
        assertSerializes( IgnoredMessage.IGNORED );
        assertSerializes( new RecordMessage( new Value[]{value( 1337L )} ) );
        assertSerializes( new SuccessMessage( new HashMap<>() ) );
    }

    @Test
    void shouldPackUnpackValidValues() throws Throwable
    {
        assertSerializesValue( value( parameters( "cat", null, "dog", null ) ) );
        assertSerializesValue( value( parameters( "k", 12, "a", "banana" ) ) );
        assertSerializesValue( value( asList( "k", 12, "a", "banana" ) ) );
    }

    @Test
    void shouldUnpackNodeRelationshipAndPath() throws Throwable
    {
        // Given
        assertOnlyDeserializesValue( emptyNodeValue() );
        assertOnlyDeserializesValue( filledNodeValue() );
        assertOnlyDeserializesValue( emptyRelationshipValue() );
        assertOnlyDeserializesValue( filledRelationshipValue() );
        assertOnlyDeserializesValue( emptyPathValue() );
        assertOnlyDeserializesValue( filledPathValue() );
    }


    @Test
    void shouldGiveHelpfulErrorOnMalformedNodeStruct() throws Throwable
    {
        // Given
        ChunkAwareByteBufOutput output = new ChunkAwareByteBufOutput();
        ByteBuf buf = Unpooled.buffer();
        output.start( buf );
        PackStream.Packer packer = new PackStream.Packer( output );

        packer.packStructHeader( 1, RecordMessage.SIGNATURE );
        packer.packListHeader( 1 );
        packer.packStructHeader( 0, CommonValueUnpacker.NODE );

        output.stop();
        BoltProtocolUtil.writeMessageBoundary( buf );

        // Expect
        ClientException error = assertThrows( ClientException.class, () -> unpack( buf, newEmbeddedChannel() ) );
        assertThat( error.getMessage(), startsWith(
                "Invalid message received, serialized NODE structures should have 3 fields, " +
                "received NODE structure has 0 fields." ) );
    }

    private void assertSerializesValue( Value value ) throws Throwable
    {
        assertSerializes( new RecordMessage( new Value[]{value} ) );
    }

    private void assertSerializes( Message message ) throws Throwable
    {
        EmbeddedChannel channel = newEmbeddedChannel( new KnowledgeableMessageFormat() );

        ByteBuf packed = pack( message, channel );
        Message unpackedMessage = unpack( packed, channel );

        assertEquals( message, unpackedMessage );
    }

    private EmbeddedChannel newEmbeddedChannel()
    {
        return newEmbeddedChannel( format );
    }

    private EmbeddedChannel newEmbeddedChannel( MessageFormat format )
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

    private void assertOnlyDeserializesValue( Value value ) throws Throwable
    {
        RecordMessage message = new RecordMessage( new Value[]{value} );
        ByteBuf packed = knowledgeablePack( message );

        EmbeddedChannel channel = newEmbeddedChannel();
        Message unpackedMessage = unpack( packed, channel );

        assertEquals( message, unpackedMessage );
    }

    private ByteBuf knowledgeablePack( Message message ) throws IOException
    {
        EmbeddedChannel channel = newEmbeddedChannel( new KnowledgeableMessageFormat() );
        assertTrue( channel.writeOutbound( message ) );

        ByteBuf[] packedMessages = channel.outboundMessages()
                .stream()
                .map( msg -> (ByteBuf) msg )
                .toArray( ByteBuf[]::new );

        return Unpooled.wrappedBuffer( packedMessages );
    }

    private void expectIOExceptionWithMessage( Value value, String errorMessage )
    {
        Map<String,Value> metadata = singletonMap( "relationship", value );
        InitMessage message = new InitMessage( "Hello", metadata );
        EmbeddedChannel channel = newEmbeddedChannel();

        EncoderException error = assertThrows( EncoderException.class, () -> pack( message, channel ) );
        Throwable cause = error.getCause();
        assertThat( cause, instanceOf( IOException.class ) );
        assertThat( cause.getMessage(), equalTo( errorMessage ) );
    }

}
