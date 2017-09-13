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
package org.neo4j.driver.internal.async.pool;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.messaging.ResetMessage;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.internal.async.ChannelAttributes.setCreationTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.setLastUsedTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.net.pooling.PoolSettings.DEFAULT_CONNECTION_ACQUISITION_TIMEOUT;
import static org.neo4j.driver.internal.net.pooling.PoolSettings.DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST;
import static org.neo4j.driver.internal.net.pooling.PoolSettings.DEFAULT_MAX_CONNECTION_POOL_SIZE;
import static org.neo4j.driver.internal.net.pooling.PoolSettings.DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE;
import static org.neo4j.driver.internal.net.pooling.PoolSettings.NOT_CONFIGURED;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class NettyChannelHealthCheckerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher dispatcher = new InboundMessageDispatcher( channel, DEV_NULL_LOGGING );

    @Before
    public void setUp() throws Exception
    {
        setMessageDispatcher( channel, dispatcher );
    }

    @After
    public void tearDown() throws Exception
    {
        channel.close();
    }

    @Test
    public void shouldDropTooOldChannelsWhenMaxLifetimeEnabled()
    {
        int maxConnectionLifetime = 1000;
        PoolSettings settings = new PoolSettings( DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE,
                DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST, maxConnectionLifetime, DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT );
        Clock clock = Clock.SYSTEM;
        NettyChannelHealthChecker healthChecker = new NettyChannelHealthChecker( settings, clock );

        setCreationTimestamp( channel, clock.millis() - maxConnectionLifetime * 2 );
        Future<Boolean> healthy = healthChecker.isHealthy( channel );

        assertThat( await( healthy ), is( false ) );
    }

    @Test
    public void shouldAllowVeryOldChannelsWhenMaxLifetimeDisabled()
    {
        PoolSettings settings = new PoolSettings( DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE,
                DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST, NOT_CONFIGURED, DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT );
        NettyChannelHealthChecker healthChecker = new NettyChannelHealthChecker( settings, Clock.SYSTEM );

        setCreationTimestamp( channel, 0 );
        Future<Boolean> healthy = healthChecker.isHealthy( channel );

        assertThat( await( healthy ), is( true ) );
    }

    @Test
    public void shouldKeepIdleConnectionWhenPingSucceeds()
    {
        testPing( true );
    }

    @Test
    public void shouldDropIdleConnectionWhenPingFails()
    {
        testPing( false );
    }

    @Test
    public void shouldKeepActiveConnections()
    {
        testActiveConnectionCheck( true );
    }

    @Test
    public void shouldDropInactiveConnections()
    {
        testActiveConnectionCheck( false );
    }

    private void testPing( boolean resetMessageSuccessful )
    {
        int idleTimeBeforeConnectionTest = 1000;
        PoolSettings settings = new PoolSettings( DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE,
                idleTimeBeforeConnectionTest, NOT_CONFIGURED, DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT );
        Clock clock = Clock.SYSTEM;
        NettyChannelHealthChecker healthChecker = new NettyChannelHealthChecker( settings, clock );

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
        PoolSettings settings = new PoolSettings( DEFAULT_MAX_IDLE_CONNECTION_POOL_SIZE,
                DEFAULT_IDLE_TIME_BEFORE_CONNECTION_TEST, NOT_CONFIGURED, DEFAULT_MAX_CONNECTION_POOL_SIZE,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT );
        Clock clock = Clock.SYSTEM;
        NettyChannelHealthChecker healthChecker = new NettyChannelHealthChecker( settings, clock );

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
}
