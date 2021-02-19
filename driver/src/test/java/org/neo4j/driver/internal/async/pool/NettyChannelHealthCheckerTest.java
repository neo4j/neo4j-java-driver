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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.messaging.request.ResetMessage;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.Value;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setCreationTimestamp;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setLastUsedTimestamp;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.pool.PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT;
import static org.neo4j.driver.internal.async.pool.PoolSettings.DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST;
import static org.neo4j.driver.internal.async.pool.PoolSettings.DEFAULT_MAX_CONNECTION_POOL_SIZE;
import static org.neo4j.driver.internal.async.pool.PoolSettings.NOT_CONFIGURED;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.util.TestUtil.await;

class NettyChannelHealthCheckerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher dispatcher = new InboundMessageDispatcher( channel, DEV_NULL_LOGGING );

    @BeforeEach
    void setUp()
    {
        setMessageDispatcher( channel, dispatcher );
    }

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldDropTooOldChannelsWhenMaxLifetimeEnabled()
    {
        int maxLifetime = 1000;
        PoolSettings settings = new PoolSettings( DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT, maxLifetime, DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST );
        Clock clock = Clock.SYSTEM;
        NettyChannelHealthChecker healthChecker = newHealthChecker( settings, clock );

        setCreationTimestamp( channel, clock.millis() - maxLifetime * 2 );
        Future<Boolean> healthy = healthChecker.isHealthy( channel );

        assertThat( await( healthy ), is( false ) );
    }

    @Test
    void shouldAllowVeryOldChannelsWhenMaxLifetimeDisabled()
    {
        PoolSettings settings = new PoolSettings( DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT, NOT_CONFIGURED, DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST );
        NettyChannelHealthChecker healthChecker = newHealthChecker( settings, Clock.SYSTEM );

        setCreationTimestamp( channel, 0 );
        Future<Boolean> healthy = healthChecker.isHealthy( channel );

        assertThat( await( healthy ), is( true ) );
    }

    @Test
    void shouldKeepIdleConnectionWhenPingSucceeds()
    {
        testPing( true );
    }

    @Test
    void shouldDropIdleConnectionWhenPingFails()
    {
        testPing( false );
    }

    @Test
    void shouldKeepActiveConnections()
    {
        testActiveConnectionCheck( true );
    }

    @Test
    void shouldDropInactiveConnections()
    {
        testActiveConnectionCheck( false );
    }

    private void testPing( boolean resetMessageSuccessful )
    {
        int idleTimeBeforeConnectionTest = 1000;
        PoolSettings settings = new PoolSettings( DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT, NOT_CONFIGURED, idleTimeBeforeConnectionTest );
        Clock clock = Clock.SYSTEM;
        NettyChannelHealthChecker healthChecker = newHealthChecker( settings, clock );

        setCreationTimestamp( channel, clock.millis() );
        setLastUsedTimestamp( channel, clock.millis() - idleTimeBeforeConnectionTest * 2 );

        Future<Boolean> healthy = healthChecker.isHealthy( channel );

        assertEquals( ResetMessage.RESET, single( channel.outboundMessages() ) );
        assertFalse( healthy.isDone() );

        if ( resetMessageSuccessful )
        {
            dispatcher.handleSuccessMessage( Collections.<String,Value>emptyMap() );
            assertThat( await( healthy ), is( true ) );
        }
        else
        {
            dispatcher.handleFailureMessage( "Neo.ClientError.General.Unknown", "Error!" );
            assertThat( await( healthy ), is( false ) );
        }
    }

    private void testActiveConnectionCheck( boolean channelActive )
    {
        PoolSettings settings = new PoolSettings( DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT, NOT_CONFIGURED, DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST );
        Clock clock = Clock.SYSTEM;
        NettyChannelHealthChecker healthChecker = newHealthChecker( settings, clock );

        setCreationTimestamp( channel, clock.millis() );

        if ( channelActive )
        {
            Future<Boolean> healthy = healthChecker.isHealthy( channel );
            assertThat( await( healthy ), is( true ) );
        }
        else
        {
            channel.close().syncUninterruptibly();
            Future<Boolean> healthy = healthChecker.isHealthy( channel );
            assertThat( await( healthy ), is( false ) );
        }
    }

    private NettyChannelHealthChecker newHealthChecker( PoolSettings settings, Clock clock )
    {
        return new NettyChannelHealthChecker( settings, clock, DEV_NULL_LOGGING );
    }
}
