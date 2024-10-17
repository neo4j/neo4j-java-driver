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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.authorizationStateListener;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.connectionId;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.connectionReadTimeout;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.lastUsedTimestamp;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.protocolVersion;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.serverAddress;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.serverAgent;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setAuthorizationStateListener;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setConnectionId;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setConnectionReadTimeout;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setCreationTimestamp;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setLastUsedTimestamp;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setProtocolVersion;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setServerAddress;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setServerAgent;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.setTerminationReason;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.terminationReason;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;

class ChannelAttributesTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @Test
    void shouldSetAndGetConnectionId() {
        setConnectionId(channel, "bolt-42");
        assertEquals("bolt-42", connectionId(channel));
    }

    @Test
    void shouldFailToSetConnectionIdTwice() {
        setConnectionId(channel, "bolt-42");

        assertThrows(IllegalStateException.class, () -> setConnectionId(channel, "bolt-4242"));
    }

    @Test
    void shouldSetAndGetProtocolVersion() {
        setProtocolVersion(channel, new BoltProtocolVersion(42, 0));
        assertEquals(new BoltProtocolVersion(42, 0), protocolVersion(channel));
    }

    @Test
    void shouldFailToSetProtocolVersionTwice() {
        setProtocolVersion(channel, new BoltProtocolVersion(42, 0));

        assertThrows(IllegalStateException.class, () -> setProtocolVersion(channel, new BoltProtocolVersion(43, 0)));
    }

    @Test
    void shouldSetAndGetServerAgent() {
        var agent = "Neo4j/4.2.5";
        setServerAgent(channel, agent);
        assertEquals(agent, serverAgent(channel));
    }

    @Test
    void shouldFailToSetServerAgentTwice() {
        var agent = "Neo4j/4.2.5";
        setServerAgent(channel, agent);

        assertThrows(IllegalStateException.class, () -> setServerAgent(channel, agent));
    }

    @Test
    void shouldSetAndGetAddress() {
        var address = new BoltServerAddress("local:42");
        setServerAddress(channel, address);
        assertEquals(address, serverAddress(channel));
    }

    @Test
    void shouldFailToSetAddressTwice() {
        setServerAddress(channel, BoltServerAddress.LOCAL_DEFAULT);

        assertThrows(IllegalStateException.class, () -> setServerAddress(channel, BoltServerAddress.LOCAL_DEFAULT));
    }

    @Test
    void shouldSetAndGetCreationTimestamp() {
        setCreationTimestamp(channel, 42L);
        assertEquals(42L, creationTimestamp(channel));
    }

    @Test
    void shouldFailToSetCreationTimestampTwice() {
        setCreationTimestamp(channel, 42L);

        assertThrows(IllegalStateException.class, () -> setCreationTimestamp(channel, 42L));
    }

    @Test
    void shouldSetAndGetLastUsedTimestamp() {
        assertNull(lastUsedTimestamp(channel));
        setLastUsedTimestamp(channel, 42L);
        assertEquals(42L, lastUsedTimestamp(channel).longValue());
    }

    @Test
    void shouldAllowSettingLastUsedTimestampMultipleTimes() {
        setLastUsedTimestamp(channel, 42L);
        setLastUsedTimestamp(channel, 4242L);
        setLastUsedTimestamp(channel, 424242L);

        assertEquals(424242L, lastUsedTimestamp(channel).longValue());
    }

    @Test
    void shouldSetAndGetMessageDispatcher() {
        var dispatcher = mock(InboundMessageDispatcher.class);
        setMessageDispatcher(channel, dispatcher);
        assertEquals(dispatcher, messageDispatcher(channel));
    }

    @Test
    void shouldFailToSetMessageDispatcherTwice() {
        setMessageDispatcher(channel, mock(InboundMessageDispatcher.class));

        assertThrows(
                IllegalStateException.class, () -> setMessageDispatcher(channel, mock(InboundMessageDispatcher.class)));
    }

    @Test
    void shouldSetAndGetTerminationReason() {
        var reason = "This channel has been terminated";
        setTerminationReason(channel, reason);
        assertEquals(reason, terminationReason(channel));
    }

    @Test
    void shouldFailToSetTerminationReasonTwice() {
        setTerminationReason(channel, "Reason 1");

        assertThrows(IllegalStateException.class, () -> setTerminationReason(channel, "Reason 2"));
    }

    @Test
    void shouldSetAndGetAuthorizationStateListener() {
        var listener = Mockito.mock(AuthorizationStateListener.class);
        setAuthorizationStateListener(channel, listener);
        assertEquals(listener, authorizationStateListener(channel));
    }

    @Test
    void shouldAllowOverridingAuthorizationStateListener() {
        var listener = Mockito.mock(AuthorizationStateListener.class);
        setAuthorizationStateListener(channel, listener);
        assertEquals(listener, authorizationStateListener(channel));
        var newListener = Mockito.mock(AuthorizationStateListener.class);
        setAuthorizationStateListener(channel, newListener);
        assertEquals(newListener, authorizationStateListener(channel));
    }

    @Test
    void shouldSetAndGetConnectionReadTimeout() {
        var timeout = 15L;
        setConnectionReadTimeout(channel, timeout);
        assertEquals(timeout, connectionReadTimeout(channel).orElse(null));
    }

    @Test
    void shouldFailToSetConnectionReadTimeoutTwice() {
        var timeout = 15L;
        setConnectionReadTimeout(channel, timeout);
        assertThrows(IllegalStateException.class, () -> setConnectionReadTimeout(channel, timeout));
    }
}
