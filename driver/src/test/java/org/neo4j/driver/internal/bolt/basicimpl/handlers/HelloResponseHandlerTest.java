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
package org.neo4j.driver.internal.bolt.basicimpl.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.connectionId;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.connectionReadTimeout;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.serverAgent;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.async.outbound.OutboundMessageHandler.NAME;

import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.ChannelErrorHandler;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.MessageFormatV3;

class HelloResponseHandlerTest {
    private static final String SERVER_AGENT = "Neo4j/4.4.0";

    private final EmbeddedChannel channel = new EmbeddedChannel();

    @BeforeEach
    void setUp() {
        setMessageDispatcher(channel, new InboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE));
        var pipeline = channel.pipeline();
        pipeline.addLast(NAME, new OutboundMessageHandler(new MessageFormatV3(), NoopLoggingProvider.INSTANCE));
        pipeline.addLast(new ChannelErrorHandler(NoopLoggingProvider.INSTANCE));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldSetServerAgentOnChannel() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata(SERVER_AGENT, "bolt-1");
        handler.onSuccess(metadata);

        assertTrue(agentFuture.isDone() && !agentFuture.isCompletedExceptionally() && !agentFuture.isCancelled());
        assertEquals(SERVER_AGENT, serverAgent(channel));
    }

    @Test
    void shouldThrowWhenServerVersionNotReturned() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata(null, "bolt-1");
        assertThrows(UntrustedServerException.class, () -> handler.onSuccess(metadata));

        assertTrue(agentFuture.isCompletedExceptionally()); // initialization failed
        assertTrue(channel.closeFuture().isDone()); // channel was closed
    }

    @Test
    void shouldThrowWhenServerVersionIsNull() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata(Values.NULL, "bolt-x");
        assertThrows(UntrustedServerException.class, () -> handler.onSuccess(metadata));

        assertTrue(agentFuture.isCompletedExceptionally()); // initialization failed
        assertTrue(channel.closeFuture().isDone()); // channel was closed
    }

    @Test
    void shouldThrowWhenServerAgentIsUnrecognised() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata("WrongServerVersion", "bolt-x");
        assertThrows(UntrustedServerException.class, () -> handler.onSuccess(metadata));

        assertTrue(agentFuture.isCompletedExceptionally()); // initialization failed
        assertTrue(channel.closeFuture().isDone()); // channel was closed
    }

    @Test
    void shouldSetConnectionIdOnChannel() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata(SERVER_AGENT, "bolt-42");
        handler.onSuccess(metadata);

        assertTrue(agentFuture.isDone() && !agentFuture.isCompletedExceptionally() && !agentFuture.isCancelled());
        assertEquals("bolt-42", connectionId(channel));
    }

    @Test
    void shouldThrowWhenConnectionIdNotReturned() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata(SERVER_AGENT, null);
        assertThrows(IllegalStateException.class, () -> handler.onSuccess(metadata));

        assertTrue(agentFuture.isCompletedExceptionally()); // initialization failed
        assertTrue(channel.closeFuture().isDone()); // channel was closed
    }

    @Test
    void shouldThrowWhenConnectionIdIsNull() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata(SERVER_AGENT, Values.NULL);
        assertThrows(IllegalStateException.class, () -> handler.onSuccess(metadata));

        assertTrue(agentFuture.isCompletedExceptionally()); // initialization failed
        assertTrue(channel.closeFuture().isDone()); // channel was closed
    }

    @Test
    void shouldCloseChannelOnFailure() throws Exception {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var error = new RuntimeException("Hi!");
        handler.onFailure(error);

        var channelCloseFuture = channel.closeFuture();
        channelCloseFuture.await(5, TimeUnit.SECONDS);

        assertTrue(channelCloseFuture.isSuccess());
        assertTrue(agentFuture.isCompletedExceptionally());
        assertEquals(
                error,
                assertThrows(CompletionException.class, agentFuture::join).getCause());
    }

    @Test
    void shouldNotThrowWhenConfigurationHintsAreAbsent() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata(SERVER_AGENT, "bolt-x");
        handler.onSuccess(metadata);

        assertTrue(agentFuture.isDone() && !agentFuture.isCompletedExceptionally() && !agentFuture.isCancelled());
        assertFalse(channel.closeFuture().isDone());
    }

    @Test
    void shouldNotThrowWhenConfigurationHintsAreEmpty() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata(SERVER_AGENT, "bolt-x", value(new HashMap<>()));
        handler.onSuccess(metadata);

        assertTrue(agentFuture.isDone() && !agentFuture.isCompletedExceptionally() && !agentFuture.isCancelled());
        assertFalse(channel.closeFuture().isDone());
    }

    @Test
    void shouldNotThrowWhenConfigurationHintsAreNull() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var metadata = metadata(SERVER_AGENT, "bolt-x", Values.NULL);
        handler.onSuccess(metadata);

        assertTrue(agentFuture.isDone() && !agentFuture.isCompletedExceptionally() && !agentFuture.isCancelled());
        assertFalse(channel.closeFuture().isDone());
    }

    @Test
    void shouldSetConnectionTimeoutHint() {
        var agentFuture = new CompletableFuture<String>();
        var latestAuth = new CompletableFuture<Long>();
        var handler = new HelloResponseHandler(agentFuture, channel, mock(Clock.class), latestAuth);

        var timeout = 15L;
        Map<String, Value> hints = new HashMap<>();
        hints.put(HelloResponseHandler.CONNECTION_RECEIVE_TIMEOUT_SECONDS_KEY, value(timeout));
        var metadata = metadata(SERVER_AGENT, "bolt-x", value(hints));
        handler.onSuccess(metadata);

        assertEquals(timeout, connectionReadTimeout(channel).orElse(null));
        assertTrue(agentFuture.isDone() && !agentFuture.isCompletedExceptionally() && !agentFuture.isCancelled());
        assertFalse(channel.closeFuture().isDone());
    }

    private static Map<String, Value> metadata(Object version, Object connectionId) {
        return metadata(version, connectionId, null);
    }

    private static Map<String, Value> metadata(Object version, Object connectionId, Value hints) {
        Map<String, Value> result = new HashMap<>();

        if (version == null) {
            result.put("server", null);
        } else if (version instanceof Value && ((Value) version).isNull()) {
            result.put("server", Values.NULL);
        } else {
            result.put("server", value(version.toString()));
        }

        if (connectionId == null) {
            result.put("connection_id", null);
        } else {
            result.put("connection_id", value(connectionId));
        }
        result.put(HelloResponseHandler.CONFIGURATION_HINTS_KEY, hints);

        return result;
    }
}
