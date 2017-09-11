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
package org.neo4j.driver.internal.async;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.async.ChannelAttributes.setAddress;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class ActiveChannelTrackerTest
{
    private final BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
    private final ActiveChannelTracker tracker = new ActiveChannelTracker( DEV_NULL_LOGGING );

    @Test
    public void shouldIncrementCountWhenChannelCreated()
    {
        Channel channel = newChannel();
        assertEquals( 0, tracker.activeChannelCount( address ) );

        tracker.channelCreated( channel );
        assertEquals( 1, tracker.activeChannelCount( address ) );
    }

    @Test
    public void shouldIncrementCountForAddress()
    {
        Channel channel1 = newChannel();
        Channel channel2 = newChannel();
        Channel channel3 = newChannel();

        assertEquals( 0, tracker.activeChannelCount( address ) );
        tracker.channelAcquired( channel1 );
        assertEquals( 1, tracker.activeChannelCount( address ) );
        tracker.channelAcquired( channel2 );
        assertEquals( 2, tracker.activeChannelCount( address ) );
        tracker.channelAcquired( channel3 );
        assertEquals( 3, tracker.activeChannelCount( address ) );
    }

    @Test
    public void shouldDecrementCountForAddress()
    {
        Channel channel1 = newChannel();
        Channel channel2 = newChannel();
        Channel channel3 = newChannel();

        tracker.channelAcquired( channel1 );
        tracker.channelAcquired( channel2 );
        tracker.channelAcquired( channel3 );
        assertEquals( 3, tracker.activeChannelCount( address ) );

        tracker.channelReleased( channel1 );
        assertEquals( 2, tracker.activeChannelCount( address ) );
        tracker.channelReleased( channel2 );
        assertEquals( 1, tracker.activeChannelCount( address ) );
        tracker.channelReleased( channel3 );
        assertEquals( 0, tracker.activeChannelCount( address ) );
    }

    @Test
    public void shouldThrowWhenDecrementingForUnknownAddress()
    {
        Channel channel = newChannel();

        try
        {
            tracker.channelReleased( channel );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void shouldReturnZeroActiveCountForUnknownAddress()
    {
        assertEquals( 0, tracker.activeChannelCount( address ) );
    }

    @Test
    public void shouldPruneForMissingAddress()
    {
        assertEquals( 0, tracker.activeChannelCount( address ) );
        tracker.prune( address );
        assertEquals( 0, tracker.activeChannelCount( address ) );
    }

    @Test
    public void shouldPruneForExistingAddress()
    {
        Channel channel1 = newChannel();
        Channel channel2 = newChannel();
        Channel channel3 = newChannel();

        tracker.channelAcquired( channel1 );
        tracker.channelAcquired( channel2 );
        tracker.channelAcquired( channel3 );

        assertEquals( 3, tracker.activeChannelCount( address ) );

        tracker.prune( address );

        assertEquals( 0, tracker.activeChannelCount( address ) );
        assertNull( await( channel1.closeFuture() ) );
        assertNull( await( channel2.closeFuture() ) );
        assertNull( await( channel3.closeFuture() ) );
    }

    private Channel newChannel()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        setAddress( channel, address );
        return channel;
    }
}
