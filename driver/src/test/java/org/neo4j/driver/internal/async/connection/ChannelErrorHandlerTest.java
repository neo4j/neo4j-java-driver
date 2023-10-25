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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setTerminationReason;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CodecException;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.async.inbound.ChannelErrorHandler;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;

class ChannelErrorHandlerTest {
    private EmbeddedChannel channel;
    private InboundMessageDispatcher messageDispatcher;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        messageDispatcher = new InboundMessageDispatcher(channel, DEV_NULL_LOGGING);
        setMessageDispatcher(channel, messageDispatcher);
        channel.pipeline().addLast(new ChannelErrorHandler(DEV_NULL_LOGGING));
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.close();
        }
    }

    @Test
    void shouldHandleChannelInactive() {
        channel.pipeline().fireChannelInactive();

        var error = messageDispatcher.currentError();

        assertThat(error, instanceOf(ServiceUnavailableException.class));
        assertThat(error.getMessage(), startsWith("Connection to the database terminated"));
        assertFalse(channel.isOpen());
    }

    @Test
    void shouldHandleChannelInactiveAfterExceptionCaught() {
        var originalError = new RuntimeException("Hi!");
        channel.pipeline().fireExceptionCaught(originalError);
        channel.pipeline().fireChannelInactive();

        var error = messageDispatcher.currentError();

        assertEquals(originalError, error);
        assertFalse(channel.isOpen());
    }

    @Test
    void shouldHandleChannelInactiveWhenTerminationReasonSet() {
        var terminationReason = "Something really bad happened";
        setTerminationReason(channel, terminationReason);

        channel.pipeline().fireChannelInactive();

        var error = messageDispatcher.currentError();

        assertThat(error, instanceOf(ServiceUnavailableException.class));
        assertThat(error.getMessage(), startsWith("Connection to the database terminated"));
        assertThat(error.getMessage(), containsString(terminationReason));
        assertFalse(channel.isOpen());
    }

    @Test
    void shouldHandleCodecException() {
        var cause = new RuntimeException("Hi!");
        var codecException = new CodecException("Unable to encode or decode message", cause);
        channel.pipeline().fireExceptionCaught(codecException);

        var error = messageDispatcher.currentError();

        assertEquals(cause, error);
        assertFalse(channel.isOpen());
    }

    @Test
    void shouldHandleCodecExceptionWithoutCause() {
        var codecException = new CodecException("Unable to encode or decode message");
        channel.pipeline().fireExceptionCaught(codecException);

        var error = messageDispatcher.currentError();

        assertEquals(codecException, error);
        assertFalse(channel.isOpen());
    }

    @Test
    void shouldHandleIOException() {
        var ioException = new IOException("Write or read failed");
        channel.pipeline().fireExceptionCaught(ioException);

        var error = messageDispatcher.currentError();

        assertThat(error, instanceOf(ServiceUnavailableException.class));
        assertEquals(ioException, error.getCause());
        assertFalse(channel.isOpen());
    }

    @Test
    void shouldHandleException() {
        var originalError = new RuntimeException("Random failure");
        channel.pipeline().fireExceptionCaught(originalError);

        var error = messageDispatcher.currentError();

        assertEquals(originalError, error);
        assertFalse(channel.isOpen());
    }

    @Test
    void shouldHandleMultipleExceptions() {
        var error1 = new RuntimeException("Failure 1");
        var error2 = new RuntimeException("Failure 2");

        channel.pipeline().fireExceptionCaught(error1);
        channel.pipeline().fireExceptionCaught(error2);

        var error = messageDispatcher.currentError();

        assertEquals(error1, error);
        assertFalse(channel.isOpen());
    }
}
