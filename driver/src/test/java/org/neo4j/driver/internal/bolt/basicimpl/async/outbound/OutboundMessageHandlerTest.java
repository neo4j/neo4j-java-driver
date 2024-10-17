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
package org.neo4j.driver.internal.bolt.basicimpl.async.outbound;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.testutil.TestUtil.assertByteBufContains;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackOutput;

class OutboundMessageHandlerTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @BeforeEach
    void setUp() {
        ChannelAttributes.setMessageDispatcher(
                channel, new InboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldOutputByteBufAsWrittenByWriterAndMessageBoundary() {
        var messageFormat = mockMessageFormatWithWriter(1, 2, 3, 4, 5);
        var handler = newHandler(messageFormat);
        channel.pipeline().addLast(handler);

        // do not care which message, writer will return predefined bytes anyway
        assertTrue(channel.writeOutbound(PULL_ALL));
        assertTrue(channel.finish());

        assertEquals(1, channel.outboundMessages().size());

        ByteBuf buf = channel.readOutbound();
        assertByteBufContains(
                buf,
                (short) 5,
                (byte) 1,
                (byte) 2,
                (byte) 3,
                (byte) 4,
                (byte) 5, // message body
                (byte) 0,
                (byte) 0 // message boundary
                );
    }

    @Test
    void shouldSupportByteArraysByDefault() {
        var handler = newHandler(new MessageFormatV3());
        channel.pipeline().addLast(handler);

        Map<String, Value> params = new HashMap<>();
        params.put("array", value(new byte[] {1, 2, 3}));

        assertTrue(channel.writeOutbound(new Query("RETURN 1", Values.value(params))));
        assertTrue(channel.finish());
    }

    private static MessageFormat mockMessageFormatWithWriter(
            @SuppressWarnings("SameParameterValue") final int... bytesToWrite) {
        var messageFormat = mock(MessageFormat.class);

        when(messageFormat.newWriter(any(PackOutput.class))).then(invocation -> {
            PackOutput output = invocation.getArgument(0);
            return mockWriter(output, bytesToWrite);
        });

        return messageFormat;
    }

    private static MessageFormat.Writer mockWriter(final PackOutput output, final int... bytesToWrite)
            throws IOException {
        var writer = mock(MessageFormat.Writer.class);

        doAnswer(invocation -> {
                    for (var b : bytesToWrite) {
                        output.writeByte((byte) b);
                    }
                    return writer;
                })
                .when(writer)
                .write(any(Message.class));

        return writer;
    }

    private static OutboundMessageHandler newHandler(MessageFormat messageFormat) {
        return new OutboundMessageHandler(messageFormat, NoopLoggingProvider.INSTANCE);
    }
}
