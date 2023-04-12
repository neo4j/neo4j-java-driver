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

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.authorizationStateListener;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.metrics.DevNullMetricsListener;
import org.neo4j.driver.internal.util.FakeClock;

class ConnectionPoolImplTest {
    private static final BoltServerAddress ADDRESS_1 = new BoltServerAddress("server:1");
    private static final BoltServerAddress ADDRESS_2 = new BoltServerAddress("server:2");
    private static final BoltServerAddress ADDRESS_3 = new BoltServerAddress("server:3");

    @Test
    void shouldDoNothingWhenRetainOnEmptyPool() {
        NettyChannelTracker nettyChannelTracker = mock(NettyChannelTracker.class);
        TestConnectionPool pool = newConnectionPool(nettyChannelTracker);

        pool.retainAll(singleton(LOCAL_DEFAULT));

        verifyNoInteractions(nettyChannelTracker);
    }

    @Test
    void shouldRetainSpecifiedAddresses() {
        NettyChannelTracker nettyChannelTracker = mock(NettyChannelTracker.class);
        TestConnectionPool pool = newConnectionPool(nettyChannelTracker);

        pool.acquire(ADDRESS_1, null);
        pool.acquire(ADDRESS_2, null);
        pool.acquire(ADDRESS_3, null);

        pool.retainAll(new HashSet<>(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3)));
        for (ExtendedChannelPool channelPool : pool.channelPoolsByAddress.values()) {
            assertFalse(channelPool.isClosed());
        }
    }

    @Test
    void shouldClosePoolsWhenRetaining() {
        NettyChannelTracker nettyChannelTracker = mock(NettyChannelTracker.class);
        TestConnectionPool pool = newConnectionPool(nettyChannelTracker);

        pool.acquire(ADDRESS_1, null);
        pool.acquire(ADDRESS_2, null);
        pool.acquire(ADDRESS_3, null);

        when(nettyChannelTracker.inUseChannelCount(ADDRESS_1)).thenReturn(2);
        when(nettyChannelTracker.inUseChannelCount(ADDRESS_2)).thenReturn(0);
        when(nettyChannelTracker.inUseChannelCount(ADDRESS_3)).thenReturn(3);

        pool.retainAll(new HashSet<>(asList(ADDRESS_1, ADDRESS_3)));
        assertFalse(pool.getPool(ADDRESS_1).isClosed());
        assertTrue(pool.getPool(ADDRESS_2).isClosed());
        assertFalse(pool.getPool(ADDRESS_3).isClosed());
    }

    @Test
    void shouldNotClosePoolsWithActiveConnectionsWhenRetaining() {
        NettyChannelTracker nettyChannelTracker = mock(NettyChannelTracker.class);
        TestConnectionPool pool = newConnectionPool(nettyChannelTracker);

        pool.acquire(ADDRESS_1, null);
        pool.acquire(ADDRESS_2, null);
        pool.acquire(ADDRESS_3, null);

        when(nettyChannelTracker.inUseChannelCount(ADDRESS_1)).thenReturn(1);
        when(nettyChannelTracker.inUseChannelCount(ADDRESS_2)).thenReturn(42);
        when(nettyChannelTracker.inUseChannelCount(ADDRESS_3)).thenReturn(0);

        pool.retainAll(singleton(ADDRESS_2));
        assertFalse(pool.getPool(ADDRESS_1).isClosed());
        assertFalse(pool.getPool(ADDRESS_2).isClosed());
        assertTrue(pool.getPool(ADDRESS_3).isClosed());
    }

    @Disabled("to fix")
    @Test
    void shouldRegisterAuthorizationStateListenerWithChannel() throws ExecutionException, InterruptedException {
        NettyChannelTracker nettyChannelTracker = mock(NettyChannelTracker.class);
        NettyChannelHealthChecker nettyChannelHealthChecker = mock(NettyChannelHealthChecker.class);
        ArgumentCaptor<Channel> channelArgumentCaptor = ArgumentCaptor.forClass(Channel.class);
        TestConnectionPool pool = newConnectionPool(nettyChannelTracker, nettyChannelHealthChecker);

        pool.acquire(ADDRESS_1, null).toCompletableFuture().get();
        verify(nettyChannelTracker).channelAcquired(channelArgumentCaptor.capture());
        Channel channel = channelArgumentCaptor.getValue();

        assertEquals(nettyChannelHealthChecker, authorizationStateListener(channel));
    }

    private static PoolSettings newSettings() {
        return new PoolSettings(10, 5000, -1, -1);
    }

    private static TestConnectionPool newConnectionPool(NettyChannelTracker nettyChannelTracker) {
        return newConnectionPool(nettyChannelTracker, mock(NettyChannelHealthChecker.class));
    }

    private static TestConnectionPool newConnectionPool(
            NettyChannelTracker nettyChannelTracker, NettyChannelHealthChecker nettyChannelHealthChecker) {
        return new TestConnectionPool(
                mock(Bootstrap.class),
                nettyChannelTracker,
                nettyChannelHealthChecker,
                newSettings(),
                DevNullMetricsListener.INSTANCE,
                DEV_NULL_LOGGING,
                new FakeClock(),
                true);
    }
}
