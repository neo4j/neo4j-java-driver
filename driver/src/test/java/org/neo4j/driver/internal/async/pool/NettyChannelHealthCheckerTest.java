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
package org.neo4j.driver.internal.async.pool;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.authContext;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setAuthContext;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setCreationTimestamp;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setLastUsedTimestamp;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setProtocolVersion;
import static org.neo4j.driver.internal.async.pool.PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT;
import static org.neo4j.driver.internal.async.pool.PoolSettings.DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST;
import static org.neo4j.driver.internal.async.pool.PoolSettings.DEFAULT_MAX_CONNECTION_POOL_SIZE;
import static org.neo4j.driver.internal.async.pool.PoolSettings.NOT_CONFIGURED;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.request.ResetMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.messaging.v41.BoltProtocolV41;
import org.neo4j.driver.internal.messaging.v42.BoltProtocolV42;
import org.neo4j.driver.internal.messaging.v43.BoltProtocolV43;
import org.neo4j.driver.internal.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.messaging.v51.BoltProtocolV51;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;

class NettyChannelHealthCheckerTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher dispatcher = new InboundMessageDispatcher(channel, DEV_NULL_LOGGING);

    @BeforeEach
    void setUp() {
        setMessageDispatcher(channel, dispatcher);
        var authContext = new AuthContext(new StaticAuthTokenManager(AuthTokens.none()));
        authContext.initiateAuth(AuthTokens.none());
        authContext.finishAuth(Clock.systemUTC().millis());
        setAuthContext(channel, authContext);
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldDropTooOldChannelsWhenMaxLifetimeEnabled() {
        var maxLifetime = 1000;
        var settings = new PoolSettings(
                DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT,
                maxLifetime,
                DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST);
        var clock = Clock.systemUTC();
        var healthChecker = newHealthChecker(settings, clock);

        setCreationTimestamp(channel, clock.millis() - maxLifetime * 2);
        var healthy = healthChecker.isHealthy(channel);

        assertThat(await(healthy), is(false));
    }

    @Test
    void shouldAllowVeryOldChannelsWhenMaxLifetimeDisabled() {
        var settings = new PoolSettings(
                DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT,
                NOT_CONFIGURED,
                DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST);
        var healthChecker = newHealthChecker(settings, Clock.systemUTC());

        setCreationTimestamp(channel, 0);
        var healthy = healthChecker.isHealthy(channel);
        channel.runPendingTasks();

        assertThat(await(healthy), is(true));
    }

    public static List<BoltProtocolVersion> boltVersionsBefore51() {
        return List.of(
                BoltProtocolV3.VERSION,
                BoltProtocolV4.VERSION,
                BoltProtocolV41.VERSION,
                BoltProtocolV42.VERSION,
                BoltProtocolV43.VERSION,
                BoltProtocolV44.VERSION,
                BoltProtocolV5.VERSION);
    }

    @ParameterizedTest
    @MethodSource("boltVersionsBefore51")
    void shouldFailAllConnectionsCreatedOnOrBeforeExpirationTimestamp(BoltProtocolVersion boltProtocolVersion) {
        var settings = new PoolSettings(
                DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT,
                NOT_CONFIGURED,
                DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST);
        var clock = mock(Clock.class);
        var healthChecker = newHealthChecker(settings, clock);

        var authToken = AuthTokens.basic("username", "password");
        var authTokenManager = mock(AuthTokenManager.class);
        given(authTokenManager.getToken()).willReturn(completedFuture(authToken));
        var channels = IntStream.range(0, 100)
                .mapToObj(i -> {
                    var channel = new EmbeddedChannel();
                    setProtocolVersion(channel, boltProtocolVersion);
                    setCreationTimestamp(channel, i);
                    var authContext = mock(AuthContext.class);
                    setAuthContext(channel, authContext);
                    given(authContext.getAuthTokenManager()).willReturn(authTokenManager);
                    given(authContext.getAuthToken()).willReturn(authToken);
                    given(authContext.getAuthTimestamp()).willReturn((long) i);
                    return channel;
                })
                .toList();

        var authorizationExpiredChannelIndex = channels.size() / 2 - 1;
        given(clock.millis()).willReturn((long) authorizationExpiredChannelIndex);
        healthChecker.onExpired();

        for (var i = 0; i < channels.size(); i++) {
            var channel = channels.get(i);
            var future = healthChecker.isHealthy(channel);
            channel.runPendingTasks();
            boolean health = Objects.requireNonNull(await(future));
            var expectedHealth = i > authorizationExpiredChannelIndex;
            assertEquals(expectedHealth, health, String.format("Channel %d has failed the check", i));
        }
    }

    @Test
    void shouldMarkForLogoffAllConnectionsCreatedOnOrBeforeExpirationTimestamp() {
        var settings = new PoolSettings(
                DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT,
                NOT_CONFIGURED,
                DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST);
        var clock = mock(Clock.class);
        var healthChecker = newHealthChecker(settings, clock);

        var authToken = AuthTokens.basic("username", "password");
        var authTokenManager = mock(AuthTokenManager.class);
        given(authTokenManager.getToken()).willReturn(completedFuture(authToken));
        var channels = IntStream.range(0, 100)
                .mapToObj(i -> {
                    var channel = new EmbeddedChannel();
                    setProtocolVersion(channel, BoltProtocolV51.VERSION);
                    setCreationTimestamp(channel, i);
                    var authContext = mock(AuthContext.class);
                    setAuthContext(channel, authContext);
                    given(authContext.getAuthTokenManager()).willReturn(authTokenManager);
                    given(authContext.getAuthToken()).willReturn(authToken);
                    given(authContext.getAuthTimestamp()).willReturn((long) i);
                    return channel;
                })
                .toList();

        var authorizationExpiredChannelIndex = channels.size() / 2 - 1;
        given(clock.millis()).willReturn((long) authorizationExpiredChannelIndex);
        healthChecker.onExpired();

        for (var i = 0; i < channels.size(); i++) {
            var channel = channels.get(i);
            var future = healthChecker.isHealthy(channel);
            channel.runPendingTasks();
            boolean health = Objects.requireNonNull(await(future));
            assertTrue(health, String.format("Channel %d has failed the check", i));
            var pendingLogoff = i <= authorizationExpiredChannelIndex;
            then(authContext(channel))
                    .should(pendingLogoff ? times(1) : never())
                    .markPendingLogoff();
        }
    }

    @Test
    void shouldUseGreatestExpirationTimestamp() {
        var settings = new PoolSettings(
                DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT,
                NOT_CONFIGURED,
                DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST);
        var clock = mock(Clock.class);
        given(clock.millis()).willReturn(0L).willReturn(100L);
        var healthChecker = newHealthChecker(settings, clock);

        var channel1 = new EmbeddedChannel();
        var channel2 = new EmbeddedChannel();
        setAuthContext(channel1, new AuthContext(new StaticAuthTokenManager(AuthTokens.none())));
        setAuthContext(channel2, new AuthContext(new StaticAuthTokenManager(AuthTokens.none())));

        healthChecker.onExpired();
        healthChecker.onExpired();

        var healthy = healthChecker.isHealthy(channel1);
        channel1.runPendingTasks();
        assertFalse(Objects.requireNonNull(await(healthy)));
        healthy = healthChecker.isHealthy(channel2);
        channel2.runPendingTasks();
        assertFalse(Objects.requireNonNull(await(healthy)));
        then(clock).should(times(2)).millis();
    }

    @Test
    void shouldKeepIdleConnectionWhenPingSucceeds() {
        testPing(true);
    }

    @Test
    void shouldDropIdleConnectionWhenPingFails() {
        testPing(false);
    }

    @Test
    void shouldKeepActiveConnections() {
        testActiveConnectionCheck(true);
    }

    @Test
    void shouldDropInactiveConnections() {
        testActiveConnectionCheck(false);
    }

    private void testPing(boolean resetMessageSuccessful) {
        var idleTimeBeforeConnectionTest = 1000;
        var settings = new PoolSettings(
                DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT,
                NOT_CONFIGURED,
                idleTimeBeforeConnectionTest);
        var clock = Clock.systemUTC();
        var healthChecker = newHealthChecker(settings, clock);

        setCreationTimestamp(channel, clock.millis());
        setLastUsedTimestamp(channel, clock.millis() - idleTimeBeforeConnectionTest * 2);

        var healthy = healthChecker.isHealthy(channel);
        channel.runPendingTasks();

        assertEquals(ResetMessage.RESET, single(channel.outboundMessages()));
        assertFalse(healthy.isDone());

        if (resetMessageSuccessful) {
            dispatcher.handleSuccessMessage(Collections.emptyMap());
            assertThat(await(healthy), is(true));
        } else {
            dispatcher.handleFailureMessage("Neo.ClientError.General.Unknown", "Error!");
            assertThat(await(healthy), is(false));
        }
    }

    private void testActiveConnectionCheck(boolean channelActive) {
        var settings = new PoolSettings(
                DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT,
                NOT_CONFIGURED,
                DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST);
        var clock = Clock.systemUTC();
        var healthChecker = newHealthChecker(settings, clock);

        setCreationTimestamp(channel, clock.millis());

        if (channelActive) {
            var healthy = healthChecker.isHealthy(channel);
            channel.runPendingTasks();
            assertThat(await(healthy), is(true));
        } else {
            channel.close().syncUninterruptibly();
            var healthy = healthChecker.isHealthy(channel);
            channel.runPendingTasks();
            assertThat(await(healthy), is(false));
        }
    }

    private NettyChannelHealthChecker newHealthChecker(PoolSettings settings, Clock clock) {
        return new NettyChannelHealthChecker(settings, clock, DEV_NULL_LOGGING);
    }
}
