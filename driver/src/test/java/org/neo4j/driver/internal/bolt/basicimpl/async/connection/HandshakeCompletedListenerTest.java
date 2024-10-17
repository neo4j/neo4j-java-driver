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
package org.neo4j.driver.internal.bolt.basicimpl.async.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setProtocolVersion;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.channel.embedded.EmbeddedChannel;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.api.BoltAgentUtil;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.HelloResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.HelloMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;

class HandshakeCompletedListenerTest {
    private static final String USER_AGENT = "user-agent";

    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFailConnectionInitializedPromiseWhenHandshakeFails() {
        var channelInitializedPromise = channel.newPromise();
        var listener = new HandshakeCompletedListener(
                Collections.emptyMap(),
                USER_AGENT,
                BoltAgentUtil.VALUE,
                RoutingContext.EMPTY,
                channelInitializedPromise,
                null,
                mock(Clock.class),
                new CompletableFuture<>());

        var handshakeCompletedPromise = channel.newPromise();
        var cause = new IOException("Bad handshake");
        handshakeCompletedPromise.setFailure(cause);

        listener.operationComplete(handshakeCompletedPromise);

        var error = assertThrows(Exception.class, () -> await(channelInitializedPromise));
        assertEquals(cause, error);
    }

    @Test
    void shouldWriteInitializationMessageInBoltV3WhenHandshakeCompleted() {
        var expectedMessage =
                new HelloMessage(USER_AGENT, null, authToken(), Collections.emptyMap(), false, null, false);
        var messageDispatcher = mock(InboundMessageDispatcher.class);
        setProtocolVersion(channel, BoltProtocolV3.VERSION);
        setMessageDispatcher(channel, messageDispatcher);

        var channelInitializedPromise = channel.newPromise();
        var listener = new HandshakeCompletedListener(
                authToken(),
                USER_AGENT,
                BoltAgentUtil.VALUE,
                RoutingContext.EMPTY,
                channelInitializedPromise,
                null,
                mock(Clock.class),
                new CompletableFuture<>());

        var handshakeCompletedPromise = channel.newPromise();
        handshakeCompletedPromise.setSuccess();

        listener.operationComplete(handshakeCompletedPromise);
        assertTrue(channel.finish());

        verify(messageDispatcher).enqueue(any((Class<? extends ResponseHandler>) HelloResponseHandler.class));
        var outboundMessage = channel.readOutbound();
        assertEquals(expectedMessage, outboundMessage);
    }

    private static Map<String, Value> authToken() {
        return Map.of("neo4j", Values.value("secret"));
    }
}
