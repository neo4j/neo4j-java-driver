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
package org.neo4j.driver.internal.bolt.basicimpl.async.inbound;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.ResetMessage.RESET;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.FailureMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.RecordMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.util.io.MessageToByteBufWriter;
import org.neo4j.driver.internal.bolt.basicimpl.util.messaging.KnowledgeableMessageFormat;

class InboundMessageHandlerTest {
    private EmbeddedChannel channel;
    private InboundMessageDispatcher messageDispatcher;
    private MessageToByteBufWriter writer;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        messageDispatcher = new InboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE);
        writer = new MessageToByteBufWriter(new KnowledgeableMessageFormat(false));
        ChannelAttributes.setMessageDispatcher(channel, messageDispatcher);

        var handler = new InboundMessageHandler(new MessageFormatV3(), NoopLoggingProvider.INSTANCE);
        channel.pipeline().addFirst(handler);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldReadSuccessMessage() {
        var responseHandler = mock(ResponseHandler.class);
        messageDispatcher.enqueue(responseHandler);

        Map<String, Value> metadata = new HashMap<>();
        metadata.put("key1", value(1));
        metadata.put("key2", value(2));
        channel.writeInbound(writer.asByteBuf(new SuccessMessage(metadata)));

        verify(responseHandler).onSuccess(metadata);
    }

    @Test
    void shouldReadFailureMessage() {
        var responseHandler = mock(ResponseHandler.class);
        messageDispatcher.enqueue(responseHandler);

        channel.writeInbound(writer.asByteBuf(new FailureMessage("Neo.TransientError.General.ReadOnly", "Hi!")));

        var captor = ArgumentCaptor.forClass(Neo4jException.class);
        verify(responseHandler).onFailure(captor.capture());
        assertEquals("Neo.TransientError.General.ReadOnly", captor.getValue().code());
        assertEquals("Hi!", captor.getValue().getMessage());
    }

    @Test
    void shouldReadRecordMessage() {
        var responseHandler = mock(ResponseHandler.class);
        messageDispatcher.enqueue(responseHandler);

        var fields = new Value[] {value(1), value(2), value(3)};
        channel.writeInbound(writer.asByteBuf(new RecordMessage(fields)));

        verify(responseHandler).onRecord(fields);
    }

    @Test
    void shouldReadIgnoredMessage() {
        var responseHandler = mock(ResponseHandler.class);
        messageDispatcher.enqueue(responseHandler);

        channel.writeInbound(writer.asByteBuf(IgnoredMessage.IGNORED));
        assertEquals(0, messageDispatcher.queuedHandlersCount());
    }

    @Test
    void shouldRethrowReadErrors() throws IOException {
        var messageFormat = mock(MessageFormat.class);
        var reader = mock(MessageFormat.Reader.class);
        var error = new RuntimeException("Unable to decode!");
        doThrow(error).when(reader).read(any());
        when(messageFormat.newReader(any())).thenReturn(reader);

        var handler = new InboundMessageHandler(messageFormat, NoopLoggingProvider.INSTANCE);

        channel.pipeline().remove(InboundMessageHandler.class);
        channel.pipeline().addLast(handler);

        var e = assertThrows(DecoderException.class, () -> channel.writeInbound(writer.asByteBuf(RESET)));
        assertThat(e.getMessage(), startsWith("Failed to read inbound message"));
    }
}
