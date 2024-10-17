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

import static io.netty.util.AttributeKey.newInstance;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltPatchesListener;

public final class ChannelAttributes {
    private static final AttributeKey<String> CONNECTION_ID = newInstance("connectionId");
    private static final AttributeKey<BoltProtocolVersion> PROTOCOL_VERSION = newInstance("protocolVersion");
    private static final AttributeKey<String> SERVER_AGENT = newInstance("serverAgent");
    private static final AttributeKey<BoltServerAddress> ADDRESS = newInstance("serverAddress");
    private static final AttributeKey<Long> CREATION_TIMESTAMP = newInstance("creationTimestamp");
    private static final AttributeKey<Long> LAST_USED_TIMESTAMP = newInstance("lastUsedTimestamp");
    private static final AttributeKey<InboundMessageDispatcher> MESSAGE_DISPATCHER = newInstance("messageDispatcher");
    private static final AttributeKey<String> TERMINATION_REASON = newInstance("terminationReason");
    private static final AttributeKey<AuthorizationStateListener> AUTHORIZATION_STATE_LISTENER =
            newInstance("authorizationStateListener");
    private static final AttributeKey<Set<BoltPatchesListener>> BOLT_PATCHES_LISTENERS =
            newInstance("boltPatchesListeners");

    // configuration hints provided by the server
    private static final AttributeKey<Long> CONNECTION_READ_TIMEOUT = newInstance("connectionReadTimeout");

    private static final AttributeKey<Boolean> TELEMETRY_ENABLED = newInstance("telemetryEnabled");

    private ChannelAttributes() {}

    public static String connectionId(Channel channel) {
        return get(channel, CONNECTION_ID);
    }

    public static void setConnectionId(Channel channel, String id) {
        setOnce(channel, CONNECTION_ID, id);
    }

    public static BoltProtocolVersion protocolVersion(Channel channel) {
        return get(channel, PROTOCOL_VERSION);
    }

    public static void setProtocolVersion(Channel channel, BoltProtocolVersion version) {
        setOnce(channel, PROTOCOL_VERSION, version);
    }

    public static void setServerAgent(Channel channel, String serverAgent) {
        setOnce(channel, SERVER_AGENT, serverAgent);
    }

    public static String serverAgent(Channel channel) {
        return get(channel, SERVER_AGENT);
    }

    public static BoltServerAddress serverAddress(Channel channel) {
        return get(channel, ADDRESS);
    }

    public static void setServerAddress(Channel channel, BoltServerAddress address) {
        setOnce(channel, ADDRESS, address);
    }

    public static long creationTimestamp(Channel channel) {
        return get(channel, CREATION_TIMESTAMP);
    }

    public static void setCreationTimestamp(Channel channel, long creationTimestamp) {
        setOnce(channel, CREATION_TIMESTAMP, creationTimestamp);
    }

    public static Long lastUsedTimestamp(Channel channel) {
        return get(channel, LAST_USED_TIMESTAMP);
    }

    public static void setLastUsedTimestamp(Channel channel, long lastUsedTimestamp) {
        set(channel, LAST_USED_TIMESTAMP, lastUsedTimestamp);
    }

    public static InboundMessageDispatcher messageDispatcher(Channel channel) {
        return get(channel, MESSAGE_DISPATCHER);
    }

    public static void setMessageDispatcher(Channel channel, InboundMessageDispatcher messageDispatcher) {
        setOnce(channel, MESSAGE_DISPATCHER, messageDispatcher);
    }

    public static String terminationReason(Channel channel) {
        return get(channel, TERMINATION_REASON);
    }

    public static void setTerminationReason(Channel channel, String reason) {
        setOnce(channel, TERMINATION_REASON, reason);
    }

    public static AuthorizationStateListener authorizationStateListener(Channel channel) {
        return get(channel, AUTHORIZATION_STATE_LISTENER);
    }

    public static void setAuthorizationStateListener(
            Channel channel, AuthorizationStateListener authorizationStateListener) {
        set(channel, AUTHORIZATION_STATE_LISTENER, authorizationStateListener);
    }

    public static Optional<Long> connectionReadTimeout(Channel channel) {
        return Optional.ofNullable(get(channel, CONNECTION_READ_TIMEOUT));
    }

    public static void setConnectionReadTimeout(Channel channel, Long connectionReadTimeout) {
        setOnce(channel, CONNECTION_READ_TIMEOUT, connectionReadTimeout);
    }

    public static void addBoltPatchesListener(Channel channel, BoltPatchesListener listener) {
        var boltPatchesListeners = get(channel, BOLT_PATCHES_LISTENERS);
        if (boltPatchesListeners == null) {
            boltPatchesListeners = new HashSet<>();
            setOnce(channel, BOLT_PATCHES_LISTENERS, boltPatchesListeners);
        }
        boltPatchesListeners.add(listener);
    }

    public static Set<BoltPatchesListener> boltPatchesListeners(Channel channel) {
        var boltPatchesListeners = get(channel, BOLT_PATCHES_LISTENERS);
        return boltPatchesListeners != null ? boltPatchesListeners : Collections.emptySet();
    }

    public static void setTelemetryEnabled(Channel channel, Boolean telemetryEnabled) {
        setOnce(channel, TELEMETRY_ENABLED, telemetryEnabled);
    }

    public static boolean telemetryEnabled(Channel channel) {
        return Optional.ofNullable(get(channel, TELEMETRY_ENABLED)).orElse(false);
    }

    private static <T> T get(Channel channel, AttributeKey<T> key) {
        return channel.attr(key).get();
    }

    private static <T> void set(Channel channel, AttributeKey<T> key, T value) {
        channel.attr(key).set(value);
    }

    private static <T> void setOnce(Channel channel, AttributeKey<T> key, T value) {
        var existingValue = channel.attr(key).setIfAbsent(value);
        if (existingValue != null) {
            throw new IllegalStateException(
                    "Unable to set " + key.name() + " because it is already set to " + existingValue);
        }
    }
}
