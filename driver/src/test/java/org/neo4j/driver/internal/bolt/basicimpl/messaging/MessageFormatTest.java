/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.bolt.basicimpl.messaging;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyPathValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledRelationshipValue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.BoltProtocolUtil;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelPipelineBuilderImpl;
import org.neo4j.driver.internal.bolt.basicimpl.async.outbound.ChunkAwareByteBufOutput;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.common.CommonValueUnpacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.FailureMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.RecordMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackStream;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.util.messaging.KnowledgeableMessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.util.messaging.MemorizingInboundMessageDispatcher;

class MessageFormatTest {
    public final MessageFormat format = new MessageFormatV3();

    @Test
    void shouldUnpackAllResponses() {
        assertSerializes(new FailureMessage("Hello", "World!"));
        assertSerializes(IgnoredMessage.IGNORED);
        assertSerializes(new RecordMessage(new Value[] {value(1337L)}));
        assertSerializes(new SuccessMessage(new HashMap<>()));
    }

    @Test
    void shouldPackUnpackValidValues() {
        assertSerializesValue(value(parameters("cat", null, "dog", null)));
        assertSerializesValue(value(parameters("k", 12, "a", "banana")));
        assertSerializesValue(value(asList("k", 12, "a", "banana")));
    }

    @Test
    void shouldUnpackNodeRelationshipAndPath() {
        // Given
        assertOnlyDeserializesValue(emptyNodeValue());
        assertOnlyDeserializesValue(filledNodeValue());
        assertOnlyDeserializesValue(emptyRelationshipValue());
        assertOnlyDeserializesValue(filledRelationshipValue());
        assertOnlyDeserializesValue(emptyPathValue());
        assertOnlyDeserializesValue(filledPathValue());
    }

    @Test
    @SuppressWarnings("ExtractMethodRecommender")
    void shouldGiveHelpfulErrorOnMalformedNodeStruct() throws Throwable {
        // Given
        var output = new ChunkAwareByteBufOutput();
        var buf = Unpooled.buffer();
        output.start(buf);
        var packer = new PackStream.Packer(output);

        packer.packStructHeader(1, RecordMessage.SIGNATURE);
        packer.packListHeader(1);
        packer.packStructHeader(0, CommonValueUnpacker.NODE);

        output.stop();
        BoltProtocolUtil.writeMessageBoundary(buf);

        var channel = newEmbeddedChannel();
        var dispatcher = messageDispatcher(channel);
        var memorizingDispatcher = ((MemorizingInboundMessageDispatcher) dispatcher);
        var errorFuture = new CompletableFuture<Void>();
        memorizingDispatcher.enqueue(new ResponseHandler() {
            @Override
            public void onSuccess(Map<String, Value> metadata) {
                errorFuture.complete(null);
            }

            @Override
            public void onFailure(Throwable error) {
                errorFuture.completeExceptionally(error);
            }

            @Override
            public void onRecord(Value[] fields) {
                // ignored
            }
        });
        channel.writeInbound(buf);

        // Expect
        Throwable error = assertThrows(CompletionException.class, errorFuture::join);
        error = assertInstanceOf(ClientException.class, error.getCause());
        assertThat(
                error.getMessage(),
                startsWith("Invalid message received, serialized NODE structures should have 3 fields, "
                        + "received NODE structure has 0 fields."));
    }

    private void assertSerializesValue(Value value) {
        assertSerializes(new RecordMessage(new Value[] {value}));
    }

    private void assertSerializes(Message message) {
        var channel = newEmbeddedChannel(new KnowledgeableMessageFormat(false));

        var packed = pack(message, channel);
        var unpackedMessage = unpack(packed, channel);

        assertEquals(message, unpackedMessage);
    }

    private EmbeddedChannel newEmbeddedChannel() {
        return newEmbeddedChannel(format);
    }

    private EmbeddedChannel newEmbeddedChannel(MessageFormat format) {
        var channel = new EmbeddedChannel();
        setMessageDispatcher(channel, new MemorizingInboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE));
        new ChannelPipelineBuilderImpl().build(format, channel.pipeline(), NoopLoggingProvider.INSTANCE);
        return channel;
    }

    private ByteBuf pack(Message message, EmbeddedChannel channel) {
        assertTrue(channel.writeOutbound(message));

        var packedMessages =
                channel.outboundMessages().stream().map(msg -> (ByteBuf) msg).toArray(ByteBuf[]::new);

        return Unpooled.wrappedBuffer(packedMessages);
    }

    private Message unpack(ByteBuf packed, EmbeddedChannel channel) {
        channel.writeInbound(packed);

        var dispatcher = messageDispatcher(channel);
        var memorizingDispatcher = ((MemorizingInboundMessageDispatcher) dispatcher);

        var unpackedMessages = memorizingDispatcher.messages();

        assertEquals(1, unpackedMessages.size());
        return unpackedMessages.get(0);
    }

    private void assertOnlyDeserializesValue(Value value) {
        var message = new RecordMessage(new Value[] {value});
        var packed = knowledgeablePack(message);

        var channel = newEmbeddedChannel();
        var unpackedMessage = unpack(packed, channel);

        assertEquals(message, unpackedMessage);
    }

    private ByteBuf knowledgeablePack(Message message) {
        var channel = newEmbeddedChannel(new KnowledgeableMessageFormat(false));
        assertTrue(channel.writeOutbound(message));

        var packedMessages =
                channel.outboundMessages().stream().map(msg -> (ByteBuf) msg).toArray(ByteBuf[]::new);

        return Unpooled.wrappedBuffer(packedMessages);
    }
}
