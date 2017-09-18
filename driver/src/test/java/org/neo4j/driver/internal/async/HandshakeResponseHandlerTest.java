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
import org.junit.Before;
import org.junit.Test;

import org.neo4j.driver.internal.async.inbound.ChunkDecoder;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.inbound.InboundMessageHandler;
import org.neo4j.driver.internal.async.inbound.MessageDecoder;
import org.neo4j.driver.internal.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.v1.exceptions.ClientException;

import static io.netty.buffer.Unpooled.copyInt;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.ProtocolUtil.HTTP;
import static org.neo4j.driver.internal.async.ProtocolUtil.NO_PROTOCOL_VERSION;
import static org.neo4j.driver.internal.async.ProtocolUtil.PROTOCOL_VERSION_1;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class HandshakeResponseHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @Before
    public void setUp() throws Exception
    {
        setMessageDispatcher( channel, new InboundMessageDispatcher( channel, DEV_NULL_LOGGING ) );
    }

    @After
    public void tearDown() throws Exception
    {
        channel.close();
    }

    @Test
    public void shouldFailGivenPromiseWhenExceptionCaught()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeResponseHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        RuntimeException cause = new RuntimeException( "Error!" );
        channel.pipeline().fireExceptionCaught( cause );

        try
        {
            // promise should fail
            await( handshakeCompletedPromise );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( cause, e );
        }

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );
    }

    @Test
    public void shouldSelectProtocolV1WhenServerSuggests()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeResponseHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        channel.pipeline().fireChannelRead( copyInt( PROTOCOL_VERSION_1 ) );

        // all inbound handlers should be set
        assertNotNull( channel.pipeline().get( ChunkDecoder.class ) );
        assertNotNull( channel.pipeline().get( MessageDecoder.class ) );
        assertNotNull( channel.pipeline().get( InboundMessageHandler.class ) );

        // all outbound handlers should be set
        assertNotNull( channel.pipeline().get( OutboundMessageHandler.class ) );

        // promise should be successful
        assertNull( await( handshakeCompletedPromise ) );
    }

    @Test
    public void shouldFailGivenPromiseWhenServerSuggestsNoProtocol()
    {
        testFailure( NO_PROTOCOL_VERSION, "The server does not support any of the protocol versions" );
    }

    @Test
    public void shouldFailGivenPromiseWhenServerSuggestsHttp()
    {
        testFailure( HTTP, "Server responded HTTP" );
    }

    @Test
    public void shouldFailGivenPromiseWhenServerSuggestsUnknownProtocol()
    {
        testFailure( 42, "Protocol error" );
    }

    private void testFailure( int serverSuggestedVersion, String expectedMessagePrefix )
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeResponseHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        channel.pipeline().fireChannelRead( copyInt( serverSuggestedVersion ) );

        try
        {
            // promise should fail
            await( handshakeCompletedPromise );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertThat( e.getMessage(), startsWith( expectedMessagePrefix ) );
        }

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );
    }

    private static HandshakeResponseHandler newHandler( ChannelPromise handshakeCompletedPromise )
    {
        return new HandshakeResponseHandler( handshakeCompletedPromise, DEV_NULL_LOGGING );
    }
}
