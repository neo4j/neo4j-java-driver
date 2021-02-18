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

import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;

import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.async.inbound.ChunkDecoder;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.inbound.InboundMessageHandler;
import org.neo4j.driver.internal.async.inbound.MessageDecoder;
import org.neo4j.driver.internal.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.messaging.v4.MessageFormatV4;
import org.neo4j.driver.internal.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.util.ErrorUtil;

import static io.netty.buffer.Unpooled.copyInt;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.driver.internal.async.connection.BoltProtocolUtil.NO_PROTOCOL_VERSION;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.util.TestUtil.await;

class HandshakeHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @BeforeEach
    void setUp()
    {
        setMessageDispatcher( channel, new InboundMessageDispatcher( channel, DEV_NULL_LOGGING ) );
    }

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFailGivenPromiseWhenExceptionCaught()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        RuntimeException cause = new RuntimeException( "Error!" );
        channel.pipeline().fireExceptionCaught( cause );

        // promise should fail
        ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, () -> await( handshakeCompletedPromise ) );
        assertEquals( cause, error.getCause() );

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );
    }

    @Test
    void shouldFailGivenPromiseWhenServiceUnavailableExceptionCaught()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        ServiceUnavailableException error = new ServiceUnavailableException( "Bad error" );
        channel.pipeline().fireExceptionCaught( error );

        // promise should fail
        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> await( handshakeCompletedPromise ) );
        assertEquals( error, e );

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );
    }

    @Test
    void shouldFailGivenPromiseWhenMultipleExceptionsCaught()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        RuntimeException error1 = new RuntimeException( "Error 1" );
        RuntimeException error2 = new RuntimeException( "Error 2" );
        channel.pipeline().fireExceptionCaught( error1 );
        channel.pipeline().fireExceptionCaught( error2 );

        // promise should fail
        ServiceUnavailableException e1 = assertThrows( ServiceUnavailableException.class, () -> await( handshakeCompletedPromise ) );
        assertEquals( error1, e1.getCause() );

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );

        RuntimeException e2 = assertThrows( RuntimeException.class, channel::checkException );
        assertEquals( error2, e2 );
    }

    @Test
    void shouldUnwrapDecoderException()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        IOException cause = new IOException( "Error!" );
        channel.pipeline().fireExceptionCaught( new DecoderException( cause ) );

        // promise should fail
        ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, () -> await( handshakeCompletedPromise ) );
        assertEquals( cause, error.getCause() );

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );
    }

    @Test
    void shouldHandleDecoderExceptionWithoutCause()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        DecoderException decoderException = new DecoderException( "Unable to decode a message" );
        channel.pipeline().fireExceptionCaught( decoderException );

        ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, () -> await( handshakeCompletedPromise ) );
        assertEquals( decoderException, error.getCause() );

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );
    }

    @Test
    void shouldTranslateSSLHandshakeException()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        SSLHandshakeException error = new SSLHandshakeException( "Invalid certificate" );
        channel.pipeline().fireExceptionCaught( error );

        // promise should fail
        SecurityException e = assertThrows( SecurityException.class, () -> await( handshakeCompletedPromise ) );
        assertEquals( error, e.getCause() );

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );
    }

    @ParameterizedTest
    @MethodSource( "protocolVersions" )
    public void testProtocolSelection( BoltProtocolVersion protocolVersion, Class<? extends MessageFormat> expectedMessageFormatClass )
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        MemorizingChannelPipelineBuilder pipelineBuilder = new MemorizingChannelPipelineBuilder();
        HandshakeHandler handler = newHandler( pipelineBuilder, handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        channel.pipeline().fireChannelRead( copyInt( protocolVersion.toInt() ) );

        // expected message format should've been used
        assertThat( pipelineBuilder.usedMessageFormat, instanceOf( expectedMessageFormatClass ) );

        // handshake handler itself should be removed
        assertNull( channel.pipeline().get( HandshakeHandler.class ) );

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
    void shouldFailGivenPromiseWhenServerSuggestsNoProtocol()
    {
        testFailure( NO_PROTOCOL_VERSION, "The server does not support any of the protocol versions" );
    }

    @Test
    void shouldFailGivenPromiseWhenServerSuggestsHttp()
    {
        testFailure( new BoltProtocolVersion( 80, 84 ), "Server responded HTTP" );
    }

    @Test
    void shouldFailGivenPromiseWhenServerSuggestsUnknownProtocol()
    {
        testFailure( new BoltProtocolVersion( 42, 0 ), "Protocol error" );
    }

    @Test
    void shouldFailGivenPromiseWhenChannelInactive()
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        channel.pipeline().fireChannelInactive();

        // promise should fail
        ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, () -> await( handshakeCompletedPromise ) );
        assertEquals( ErrorUtil.newConnectionTerminatedError().getMessage(), error.getMessage() );

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );
    }

    private void testFailure( BoltProtocolVersion serverSuggestedVersion, String expectedMessagePrefix )
    {
        ChannelPromise handshakeCompletedPromise = channel.newPromise();
        HandshakeHandler handler = newHandler( handshakeCompletedPromise );
        channel.pipeline().addLast( handler );

        channel.pipeline().fireChannelRead( copyInt( serverSuggestedVersion.toInt() ) );

        // handshake handler itself should be removed
        assertNull( channel.pipeline().get( HandshakeHandler.class ) );

        // promise should fail
        Exception error = assertThrows( Exception.class, () -> await( handshakeCompletedPromise ) );
        assertThat( error, instanceOf( ClientException.class ) );
        assertThat( error.getMessage(), startsWith( expectedMessagePrefix ) );

        // channel should be closed
        assertNull( await( channel.closeFuture() ) );
    }

    private static Stream<Arguments> protocolVersions()
    {
        return Stream.of( arguments( BoltProtocolV3.VERSION, MessageFormatV3.class ),
                          arguments( BoltProtocolV4.VERSION, MessageFormatV4.class ),
                          arguments( BoltProtocolV41.VERSION, MessageFormatV4.class ),
                          arguments( BoltProtocolV42.VERSION, MessageFormatV4.class ) );
    }

    private static HandshakeHandler newHandler( ChannelPromise handshakeCompletedPromise )
    {
        return newHandler( new ChannelPipelineBuilderImpl(), handshakeCompletedPromise );
    }

    private static HandshakeHandler newHandler( ChannelPipelineBuilder pipelineBuilder, ChannelPromise handshakeCompletedPromise )
    {
        return new HandshakeHandler( pipelineBuilder, handshakeCompletedPromise, DEV_NULL_LOGGING );
    }

    private static class MemorizingChannelPipelineBuilder extends ChannelPipelineBuilderImpl
    {
        MessageFormat usedMessageFormat;

        @Override
        public void build( MessageFormat messageFormat, ChannelPipeline pipeline, Logging logging )
        {
            usedMessageFormat = messageFormat;
            super.build( messageFormat, pipeline, logging );
        }
    }
}
