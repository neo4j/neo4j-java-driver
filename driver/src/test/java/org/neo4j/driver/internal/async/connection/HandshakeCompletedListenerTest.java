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
package org.neo4j.driver.internal.async.connection;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setAuthContext;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setProtocolVersion;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.channel.embedded.EmbeddedChannel;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.internal.BoltAgentUtil;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.pool.AuthContext;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.handlers.HelloResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Futures;

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
                USER_AGENT,
                BoltAgentUtil.VALUE,
                RoutingContext.EMPTY,
                channelInitializedPromise,
                null,
                mock(Clock.class));

        var handshakeCompletedPromise = channel.newPromise();
        var cause = new IOException("Bad handshake");
        handshakeCompletedPromise.setFailure(cause);

        listener.operationComplete(handshakeCompletedPromise);

        var error = assertThrows(Exception.class, () -> await(channelInitializedPromise));
        assertEquals(cause, error);
    }

    @Test
    void shouldWriteInitializationMessageInBoltV3WhenHandshakeCompleted() {
        var authTokenManager = mock(AuthTokenManager.class);
        var authToken = authToken();
        given(authTokenManager.getToken()).willReturn(completedFuture(authToken));
        var authContext = mock(AuthContext.class);
        given(authContext.getAuthTokenManager()).willReturn(authTokenManager);
        setAuthContext(channel, authContext);
        testWritingOfInitializationMessage(
                new HelloMessage(USER_AGENT, null, authToken().toMap(), Collections.emptyMap(), false, null));
        then(authContext).should().initiateAuth(authToken);
    }

    @Test
    void shouldFailPromiseWhenTokenStageCompletesExceptionally() {
        // given
        var channelInitializedPromise = channel.newPromise();
        var listener = new HandshakeCompletedListener(
                USER_AGENT,
                BoltAgentUtil.VALUE,
                mock(RoutingContext.class),
                channelInitializedPromise,
                null,
                mock(Clock.class));
        var handshakeCompletedPromise = channel.newPromise();
        handshakeCompletedPromise.setSuccess();
        setProtocolVersion(channel, BoltProtocolV5.VERSION);
        var authContext = mock(AuthContext.class);
        setAuthContext(channel, authContext);
        var authTokeManager = mock(AuthTokenManager.class);
        given(authContext.getAuthTokenManager()).willReturn(authTokeManager);
        var exception = mock(Throwable.class);
        given(authTokeManager.getToken()).willReturn(failedFuture(exception));

        // when
        listener.operationComplete(handshakeCompletedPromise);
        channel.runPendingTasks();

        // then
        var future = Futures.asCompletionStage(channelInitializedPromise).toCompletableFuture();
        var actualException =
                assertThrows(CompletionException.class, future::join).getCause();
        assertEquals(exception, actualException);
    }

    private void testWritingOfInitializationMessage(Message expectedMessage) {
        var messageDispatcher = mock(InboundMessageDispatcher.class);
        setProtocolVersion(channel, BoltProtocolV3.VERSION);
        setMessageDispatcher(channel, messageDispatcher);

        var channelInitializedPromise = channel.newPromise();
        var listener = new HandshakeCompletedListener(
                USER_AGENT,
                BoltAgentUtil.VALUE,
                RoutingContext.EMPTY,
                channelInitializedPromise,
                null,
                mock(Clock.class));

        var handshakeCompletedPromise = channel.newPromise();
        handshakeCompletedPromise.setSuccess();

        listener.operationComplete(handshakeCompletedPromise);
        assertTrue(channel.finish());

        verify(messageDispatcher).enqueue(any((Class<? extends ResponseHandler>) HelloResponseHandler.class));
        var outboundMessage = channel.readOutbound();
        assertEquals(expectedMessage, outboundMessage);
    }

    private static InternalAuthToken authToken() {
        return (InternalAuthToken) AuthTokens.basic("neo4j", "secret");
    }
}
