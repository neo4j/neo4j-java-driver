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

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import org.bouncycastle.util.Arrays;
import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setProtocolVersion;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setServerAddress;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.metrics.InternalAbstractMetrics.DEV_NULL_METRICS;

class NettyChannelTrackerTest
{
    private final BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
    private final NettyChannelTracker tracker = new NettyChannelTracker( DEV_NULL_METRICS, mock( ChannelGroup.class ), DEV_NULL_LOGGING );

    @Test
    void shouldIncrementIdleCountWhenChannelCreated()
    {
        Channel channel = newChannel();
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );

        tracker.channelCreated( channel, null );
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 1, tracker.idleChannelCount( address ) );
    }

    @Test
    void shouldIncrementInUseCountWhenChannelAcquired()
    {
        Channel channel = newChannel();
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );

        tracker.channelCreated( channel, null );
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 1, tracker.idleChannelCount( address ) );

        tracker.channelAcquired( channel );
        assertEquals( 1, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );
    }

    @Test
    void shouldIncrementIdleCountWhenChannelReleased()
    {
        Channel channel = newChannel();
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );

        channelCreatedAndAcquired( channel );
        assertEquals( 1, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );

        tracker.channelReleased( channel );
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 1, tracker.idleChannelCount( address ) );
    }

    @Test
    void shouldIncrementIdleCountForAddress()
    {
        Channel channel1 = newChannel();
        Channel channel2 = newChannel();
        Channel channel3 = newChannel();

        assertEquals( 0, tracker.idleChannelCount( address ) );
        tracker.channelCreated( channel1, null );
        assertEquals( 1, tracker.idleChannelCount( address ) );
        tracker.channelCreated( channel2, null );
        assertEquals( 2, tracker.idleChannelCount( address ) );
        tracker.channelCreated( channel3, null );
        assertEquals( 3, tracker.idleChannelCount( address ) );
        assertEquals( 0, tracker.inUseChannelCount( address ) );
    }

    @Test
    void shouldDecrementCountForAddress()
    {
        Channel channel1 = newChannel();
        Channel channel2 = newChannel();
        Channel channel3 = newChannel();

        channelCreatedAndAcquired( channel1 );
        channelCreatedAndAcquired( channel2 );
        channelCreatedAndAcquired( channel3 );
        assertEquals( 3, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );

        tracker.channelReleased( channel1 );
        assertEquals( 2, tracker.inUseChannelCount( address ) );
        assertEquals( 1, tracker.idleChannelCount( address ) );
        tracker.channelReleased( channel2 );
        assertEquals( 1, tracker.inUseChannelCount( address ) );
        assertEquals( 2, tracker.idleChannelCount( address ) );
        tracker.channelReleased( channel3 );
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 3, tracker.idleChannelCount( address ) );
    }

    @Test
    void shouldDecreaseIdleWhenClosedOutsidePool() throws Throwable
    {
        // Given
        Channel channel = newChannel();
        channelCreatedAndAcquired( channel );
        assertEquals( 1, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );

        // When closed before session.close
        channel.close().sync();

        // Then
        assertEquals( 1, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );

        tracker.channelReleased( channel );
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );
    }

    @Test
    void shouldDecreaseIdleWhenClosedInsidePool() throws Throwable
    {
        // Given
        Channel channel = newChannel();
        channelCreatedAndAcquired( channel );
        assertEquals( 1, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );

        tracker.channelReleased( channel );
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 1, tracker.idleChannelCount( address ) );

        // When closed before acquire
        channel.close().sync();
        // Then
        assertEquals( 0, tracker.inUseChannelCount( address ) );
        assertEquals( 0, tracker.idleChannelCount( address ) );
    }

    @Test
    void shouldThrowWhenDecrementingForUnknownAddress()
    {
        Channel channel = newChannel();

        assertThrows( IllegalStateException.class, () -> tracker.channelReleased( channel ) );
    }

    @Test
    void shouldReturnZeroActiveCountForUnknownAddress()
    {
        assertEquals( 0, tracker.inUseChannelCount( address ) );
    }

    @Test
    void shouldAddChannelToGroupWhenChannelCreated()
    {
        Channel channel = newChannel();
        Channel anotherChannel = newChannel();
        ChannelGroup group = mock( ChannelGroup.class );
        NettyChannelTracker tracker = new NettyChannelTracker( DEV_NULL_METRICS, group, DEV_NULL_LOGGING );

        tracker.channelCreated( channel, null );
        tracker.channelCreated( anotherChannel, null );

        verify( group ).add( channel );
        verify( group ).add( anotherChannel );
    }

    @Test
    void shouldDelegateToProtocolPrepareToClose()
    {
        EmbeddedChannel channel = newChannelWithProtocolV3();
        EmbeddedChannel anotherChannel = newChannelWithProtocolV3();
        ChannelGroup group = mock( ChannelGroup.class );
        when( group.iterator() ).thenReturn( new Arrays.Iterator<>( new Channel[]{channel, anotherChannel} ) );

        NettyChannelTracker tracker = new NettyChannelTracker( DEV_NULL_METRICS, group, DEV_NULL_LOGGING );

        tracker.prepareToCloseChannels();

        assertThat( channel.outboundMessages().size(), equalTo( 1 ) );
        assertThat( channel.outboundMessages(), hasItem( GoodbyeMessage.GOODBYE ) );

        assertThat( anotherChannel.outboundMessages().size(), equalTo( 1 ) );
        assertThat( anotherChannel.outboundMessages(), hasItem( GoodbyeMessage.GOODBYE ) );
    }

    private Channel newChannel()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        setServerAddress( channel, address );
        return channel;
    }

    private EmbeddedChannel newChannelWithProtocolV3()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        setServerAddress( channel, address );
        setProtocolVersion( channel, BoltProtocolV3.VERSION );
        setMessageDispatcher( channel, mock( InboundMessageDispatcher.class ) );
        return channel;
    }

    private void channelCreatedAndAcquired( Channel channel )
    {
        tracker.channelCreated( channel, null );
        tracker.channelAcquired( channel );
    }
}
