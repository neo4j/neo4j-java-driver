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
package org.neo4j.driver.internal.async.connection;

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.handlers.HelloResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.spi.ResponseHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setProtocolVersion;
import static org.neo4j.driver.util.TestUtil.await;

class HandshakeCompletedListenerTest
{
    private static final String USER_AGENT = "user-agent";

    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFailConnectionInitializedPromiseWhenHandshakeFails()
    {
        ChannelPromise channelInitializedPromise = channel.newPromise();
        HandshakeCompletedListener listener = new HandshakeCompletedListener( "user-agent", authToken(), RoutingContext.EMPTY,
                channelInitializedPromise );

        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        IOException cause = new IOException( "Bad handshake" );
        handshakeCompletedPromise.setFailure( cause );

        listener.operationComplete( handshakeCompletedPromise );

        Exception error = assertThrows( Exception.class, () -> await( channelInitializedPromise ) );
        assertEquals( cause, error );
    }

    @Test
    void shouldWriteInitializationMessageInBoltV3WhenHandshakeCompleted()
    {
        testWritingOfInitializationMessage( BoltProtocolV3.VERSION, new HelloMessage( USER_AGENT, authToken().toMap(), Collections.emptyMap() ), HelloResponseHandler.class );
    }

    private void testWritingOfInitializationMessage( BoltProtocolVersion protocolVersion, Message expectedMessage, Class<? extends ResponseHandler> handlerType )
    {
        InboundMessageDispatcher messageDispatcher = mock( InboundMessageDispatcher.class );
        setProtocolVersion( channel, protocolVersion );
        setMessageDispatcher( channel, messageDispatcher );

        ChannelPromise channelInitializedPromise = channel.newPromise();
        HandshakeCompletedListener listener = new HandshakeCompletedListener( USER_AGENT, authToken(), RoutingContext.EMPTY,
                                                                              channelInitializedPromise );

        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        handshakeCompletedPromise.setSuccess();

        listener.operationComplete( handshakeCompletedPromise );
        assertTrue( channel.finish() );

        verify( messageDispatcher ).enqueue( any( handlerType ) );
        Object outboundMessage = channel.readOutbound();
        assertEquals( expectedMessage, outboundMessage );
    }

    private static InternalAuthToken authToken()
    {
        return (InternalAuthToken) AuthTokens.basic( "neo4j", "secret" );
    }
}
