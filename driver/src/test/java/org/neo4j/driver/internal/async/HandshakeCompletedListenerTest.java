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
package org.neo4j.driver.internal.async;

import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.handlers.AsyncInitResponseHandler;
import org.neo4j.driver.internal.messaging.InitMessage;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class HandshakeCompletedListenerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @After
    public void tearDown() throws Exception
    {
        channel.close();
    }

    @Test
    public void shouldFailConnectionInitializedPromiseWhenHandshakeFails()
    {
        ChannelPromise channelInitializedPromise = channel.newPromise();
        HandshakeCompletedListener listener = new HandshakeCompletedListener( "user-agent", authToken(),
                channelInitializedPromise );

        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        IOException cause = new IOException( "Bad handshake" );
        handshakeCompletedPromise.setFailure( cause );

        listener.operationComplete( handshakeCompletedPromise );

        try
        {
            await( channelInitializedPromise );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( cause, e );
        }
    }

    @Test
    public void shouldWriteInitMessageWhenHandshakeCompleted()
    {
        InboundMessageDispatcher messageDispatcher = mock( InboundMessageDispatcher.class );
        setMessageDispatcher( channel, messageDispatcher );

        ChannelPromise channelInitializedPromise = channel.newPromise();
        HandshakeCompletedListener listener = new HandshakeCompletedListener( "user-agent", authToken(),
                channelInitializedPromise );

        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        handshakeCompletedPromise.setSuccess();

        listener.operationComplete( handshakeCompletedPromise );
        assertTrue( channel.finish() );

        verify( messageDispatcher ).queue( any( AsyncInitResponseHandler.class ) );
        Object outboundMessage = channel.readOutbound();
        assertThat( outboundMessage, instanceOf( InitMessage.class ) );
        InitMessage initMessage = (InitMessage) outboundMessage;
        assertEquals( "user-agent", initMessage.userAgent() );
        assertEquals( authToken(), initMessage.authToken() );
    }

    private static Map<String,Value> authToken()
    {
        Map<String,Value> authToken = new HashMap<>();
        authToken.put( "username", value( "neo4j" ) );
        authToken.put( "username", value( "secret" ) );
        return authToken;
    }
}
