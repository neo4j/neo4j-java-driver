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

import static io.netty.buffer.Unpooled.copyInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.BoltProtocolUtil.NO_PROTOCOL_VERSION;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import java.io.IOException;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.bolt.NoopLoggingProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.ChunkDecoder;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.MessageDecoder;
import org.neo4j.driver.internal.bolt.basicimpl.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.MessageFormatV3;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v4.MessageFormatV4;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.util.ErrorUtil;

class HandshakeHandlerTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @BeforeEach
    void setUp() {
        setMessageDispatcher(channel, new InboundMessageDispatcher(channel, NoopLoggingProvider.INSTANCE));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFailGivenPromiseWhenExceptionCaught() {
        var handshakeCompletedPromise = channel.newPromise();
        var handler = newHandler(handshakeCompletedPromise);
        channel.pipeline().addLast(handler);

        var cause = new RuntimeException("Error!");
        channel.pipeline().fireExceptionCaught(cause);

        // promise should fail
        var error = assertThrows(ServiceUnavailableException.class, () -> await(handshakeCompletedPromise));
        assertEquals(cause, error.getCause());

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @Test
    void shouldFailGivenPromiseWhenServiceUnavailableExceptionCaught() {
        var handshakeCompletedPromise = channel.newPromise();
        var handler = newHandler(handshakeCompletedPromise);
        channel.pipeline().addLast(handler);

        var error = new ServiceUnavailableException("Bad error");
        channel.pipeline().fireExceptionCaught(error);

        // promise should fail
        var e = assertThrows(ServiceUnavailableException.class, () -> await(handshakeCompletedPromise));
        assertEquals(error, e);

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @Test
    void shouldFailGivenPromiseWhenMultipleExceptionsCaught() {
        var handshakeCompletedPromise = channel.newPromise();
        var handler = newHandler(handshakeCompletedPromise);
        channel.pipeline().addLast(handler);

        var error1 = new RuntimeException("Error 1");
        var error2 = new RuntimeException("Error 2");
        channel.pipeline().fireExceptionCaught(error1);
        channel.pipeline().fireExceptionCaught(error2);

        // promise should fail
        var e1 = assertThrows(ServiceUnavailableException.class, () -> await(handshakeCompletedPromise));
        assertEquals(error1, e1.getCause());

        // channel should be closed
        assertNull(await(channel.closeFuture()));

        var e2 = assertThrows(RuntimeException.class, channel::checkException);
        assertEquals(error2, e2);
    }

    @Test
    void shouldUnwrapDecoderException() {
        var handshakeCompletedPromise = channel.newPromise();
        var handler = newHandler(handshakeCompletedPromise);
        channel.pipeline().addLast(handler);

        var cause = new IOException("Error!");
        channel.pipeline().fireExceptionCaught(new DecoderException(cause));

        // promise should fail
        var error = assertThrows(ServiceUnavailableException.class, () -> await(handshakeCompletedPromise));
        assertEquals(cause, error.getCause());

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @Test
    void shouldHandleDecoderExceptionWithoutCause() {
        var handshakeCompletedPromise = channel.newPromise();
        var handler = newHandler(handshakeCompletedPromise);
        channel.pipeline().addLast(handler);

        var decoderException = new DecoderException("Unable to decode a message");
        channel.pipeline().fireExceptionCaught(decoderException);

        var error = assertThrows(ServiceUnavailableException.class, () -> await(handshakeCompletedPromise));
        assertEquals(decoderException, error.getCause());

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @Test
    void shouldTranslateSSLHandshakeException() {
        var handshakeCompletedPromise = channel.newPromise();
        var handler = newHandler(handshakeCompletedPromise);
        channel.pipeline().addLast(handler);

        var error = new SSLHandshakeException("Invalid certificate");
        channel.pipeline().fireExceptionCaught(error);

        // promise should fail
        var e = assertThrows(SecurityException.class, () -> await(handshakeCompletedPromise));
        assertEquals(error, e.getCause());

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    @ParameterizedTest
    @MethodSource("protocolVersions")
    public void testProtocolSelection(
            BoltProtocolVersion protocolVersion, Class<? extends MessageFormat> expectedMessageFormatClass) {
        var handshakeCompletedPromise = channel.newPromise();
        var pipelineBuilder = new MemorizingChannelPipelineBuilder();
        var handler = newHandler(pipelineBuilder, handshakeCompletedPromise);
        channel.pipeline().addLast(handler);

        channel.pipeline().fireChannelRead(copyInt(protocolVersion.toInt()));

        // expected message format should've been used
        assertThat(pipelineBuilder.usedMessageFormat, instanceOf(expectedMessageFormatClass));

        // handshake handler itself should be removed
        assertNull(channel.pipeline().get(HandshakeHandler.class));

        // all inbound handlers should be set
        assertNotNull(channel.pipeline().get(ChunkDecoder.class));
        assertNotNull(channel.pipeline().get(MessageDecoder.class));
        assertNotNull(channel.pipeline().get(InboundMessageHandler.class));

        // all outbound handlers should be set
        assertNotNull(channel.pipeline().get(OutboundMessageHandler.class));

        // promise should be successful
        assertNull(await(handshakeCompletedPromise));
    }

    @Test
    void shouldFailGivenPromiseWhenServerSuggestsNoProtocol() {
        testFailure(NO_PROTOCOL_VERSION, "The server does not support any of the protocol versions");
    }

    @Test
    void shouldFailGivenPromiseWhenServerSuggestsHttp() {
        testFailure(new BoltProtocolVersion(80, 84), "Server responded HTTP");
    }

    @Test
    void shouldFailGivenPromiseWhenServerSuggestsUnknownProtocol() {
        testFailure(new BoltProtocolVersion(42, 0), "Protocol error");
    }

    @Test
    void shouldFailGivenPromiseWhenChannelInactive() {
        var handshakeCompletedPromise = channel.newPromise();
        var handler = newHandler(handshakeCompletedPromise);
        channel.pipeline().addLast(handler);

        channel.pipeline().fireChannelInactive();

        // promise should fail
        var error = assertThrows(ServiceUnavailableException.class, () -> await(handshakeCompletedPromise));
        assertEquals(ErrorUtil.newConnectionTerminatedError().getMessage(), error.getMessage());

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    private void testFailure(BoltProtocolVersion serverSuggestedVersion, String expectedMessagePrefix) {
        var handshakeCompletedPromise = channel.newPromise();
        var handler = newHandler(handshakeCompletedPromise);
        channel.pipeline().addLast(handler);

        channel.pipeline().fireChannelRead(copyInt(serverSuggestedVersion.toInt()));

        // handshake handler itself should be removed
        assertNull(channel.pipeline().get(HandshakeHandler.class));

        // promise should fail
        var error = assertThrows(Exception.class, () -> await(handshakeCompletedPromise));
        assertThat(error, instanceOf(ClientException.class));
        assertThat(error.getMessage(), startsWith(expectedMessagePrefix));

        // channel should be closed
        assertNull(await(channel.closeFuture()));
    }

    private static Stream<Arguments> protocolVersions() {
        return Stream.of(
                arguments(BoltProtocolV3.VERSION, MessageFormatV3.class),
                arguments(BoltProtocolV4.VERSION, MessageFormatV4.class),
                arguments(BoltProtocolV41.VERSION, MessageFormatV4.class),
                arguments(BoltProtocolV42.VERSION, MessageFormatV4.class));
    }

    private static HandshakeHandler newHandler(ChannelPromise handshakeCompletedPromise) {
        return newHandler(new ChannelPipelineBuilderImpl(), handshakeCompletedPromise);
    }

    private static HandshakeHandler newHandler(
            ChannelPipelineBuilder pipelineBuilder, ChannelPromise handshakeCompletedPromise) {
        return new HandshakeHandler(pipelineBuilder, handshakeCompletedPromise, NoopLoggingProvider.INSTANCE);
    }

    private static class MemorizingChannelPipelineBuilder extends ChannelPipelineBuilderImpl {
        MessageFormat usedMessageFormat;

        @Override
        public void build(MessageFormat messageFormat, ChannelPipeline pipeline, LoggingProvider logging) {
            usedMessageFormat = messageFormat;
            super.build(messageFormat, pipeline, logging);
        }
    }
}
