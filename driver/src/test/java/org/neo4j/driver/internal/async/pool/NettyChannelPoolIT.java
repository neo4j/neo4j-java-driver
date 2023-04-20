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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.invocation.InvocationOnMock;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DefaultDomainNameResolver;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnectorImpl;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.metrics.DevNullMetricsListener;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.ImmediateSchedulingEventExecutor;
import org.neo4j.driver.internal.util.Neo4jFeature;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class NettyChannelPoolIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Bootstrap bootstrap;
    private NettyChannelTracker poolHandler;
    private NettyChannelPool pool;

    private static Object answer(InvocationOnMock a) {
        return ChannelHealthChecker.ACTIVE.isHealthy(a.getArgument(0));
    }

    @BeforeEach
    void setUp() {
        bootstrap = BootstrapFactory.newBootstrap(1);
        poolHandler = mock(NettyChannelTracker.class);
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
        if (bootstrap != null) {
            bootstrap.config().group().shutdownGracefully().syncUninterruptibly();
        }
    }

    @Test
    void shouldAcquireAndReleaseWithCorrectCredentials() {
        pool = newPool(neo4j.authTokenManager());

        Channel channel = await(pool.acquire(null));
        assertNotNull(channel);
        verify(poolHandler).channelCreated(eq(channel), any());
        verify(poolHandler, never()).channelReleased(channel);

        await(pool.release(channel));
        verify(poolHandler).channelReleased(channel);
    }

    @DisabledOnNeo4jWith(Neo4jFeature.BOLT_V51)
    @Test
    void shouldFailToAcquireWithWrongCredentialsBolt50AndBelow() {
        pool = newPool(new StaticAuthTokenManager(AuthTokens.basic("wrong", "wrong")));

        assertThrows(AuthenticationException.class, () -> await(pool.acquire(null)));

        verify(poolHandler, never()).channelCreated(any());
        verify(poolHandler, never()).channelReleased(any());
    }

    @EnabledOnNeo4jWith(Neo4jFeature.BOLT_V51)
    @Test
    void shouldFailToAcquireWithWrongCredentials() {
        pool = newPool(new StaticAuthTokenManager(AuthTokens.basic("wrong", "wrong")));

        assertThrows(AuthenticationException.class, () -> await(pool.acquire(null)));

        verify(poolHandler).channelCreated(any(), any());
        verify(poolHandler).channelReleased(any());
    }

    @Test
    void shouldAllowAcquireAfterFailures() throws Exception {
        int maxConnections = 2;

        Map<String, Value> authTokenMap = new HashMap<>();
        authTokenMap.put("scheme", value("basic"));
        authTokenMap.put("principal", value("neo4j"));
        authTokenMap.put("credentials", value("wrong"));
        InternalAuthToken authToken = new InternalAuthToken(authTokenMap);

        pool = newPool(new StaticAuthTokenManager(authToken), maxConnections);

        for (int i = 0; i < maxConnections; i++) {
            AuthenticationException e = assertThrows(AuthenticationException.class, () -> acquire(pool));
        }

        authTokenMap.put("credentials", value(neo4j.adminPassword()));

        assertNotNull(acquire(pool));
    }

    @Test
    void shouldLimitNumberOfConcurrentConnections() throws Exception {
        int maxConnections = 5;
        pool = newPool(neo4j.authTokenManager(), maxConnections);

        for (int i = 0; i < maxConnections; i++) {
            assertNotNull(acquire(pool));
        }

        TimeoutException e = assertThrows(TimeoutException.class, () -> acquire(pool));
        assertEquals(e.getMessage(), "Acquire operation took longer then configured maximum time");
    }

    @Test
    void shouldTrackActiveChannels() throws Exception {
        NettyChannelTracker tracker = new NettyChannelTracker(
                DevNullMetricsListener.INSTANCE, new ImmediateSchedulingEventExecutor(), DEV_NULL_LOGGING);

        poolHandler = tracker;
        pool = newPool(neo4j.authTokenManager());

        Channel channel1 = acquire(pool);
        Channel channel2 = acquire(pool);
        Channel channel3 = acquire(pool);
        assertEquals(3, tracker.inUseChannelCount(neo4j.address()));

        release(channel1);
        release(channel2);
        release(channel3);
        assertEquals(0, tracker.inUseChannelCount(neo4j.address()));

        assertNotNull(acquire(pool));
        assertNotNull(acquire(pool));
        assertEquals(2, tracker.inUseChannelCount(neo4j.address()));
    }

    private NettyChannelPool newPool(AuthTokenManager authTokenManager) {
        return newPool(authTokenManager, 100);
    }

    private NettyChannelPool newPool(AuthTokenManager authTokenManager, int maxConnections) {
        ConnectionSettings settings = new ConnectionSettings(authTokenManager, "test", 5_000);
        ChannelConnectorImpl connector = new ChannelConnectorImpl(
                settings,
                SecurityPlanImpl.insecure(),
                DEV_NULL_LOGGING,
                new FakeClock(),
                RoutingContext.EMPTY,
                DefaultDomainNameResolver.getInstance(),
                null,
                "agent");
        var nettyChannelHealthChecker = mock(NettyChannelHealthChecker.class);
        when(nettyChannelHealthChecker.isHealthy(any())).thenAnswer(NettyChannelPoolIT::answer);
        return new NettyChannelPool(
                neo4j.address(),
                connector,
                bootstrap,
                poolHandler,
                nettyChannelHealthChecker,
                1_000,
                maxConnections,
                Clock.systemUTC());
    }

    private static Channel acquire(NettyChannelPool pool) {
        return await(pool.acquire(null));
    }

    private void release(Channel channel) {
        await(pool.release(channel));
    }
}
