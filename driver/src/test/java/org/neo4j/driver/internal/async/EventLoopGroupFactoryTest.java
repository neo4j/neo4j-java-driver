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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.util.Iterables.count;
import static org.neo4j.driver.internal.util.Matchers.blockingOperationInEventLoopError;

public class EventLoopGroupFactoryTest
{
    private EventLoopGroup eventLoopGroup;

    @After
    public void tearDown()
    {
        if ( eventLoopGroup != null )
        {
            eventLoopGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    @Test
    public void shouldReturnCorrectChannelClass()
    {
        assertEquals( NioSocketChannel.class, EventLoopGroupFactory.channelClass() );
    }

    @Test
    public void shouldCreateEventLoopGroupWithSpecifiedThreadCount()
    {
        int threadCount = 2;
        eventLoopGroup = EventLoopGroupFactory.newEventLoopGroup( threadCount );
        assertEquals( threadCount, count( eventLoopGroup ) );
        assertThat( eventLoopGroup, instanceOf( NioEventLoopGroup.class ) );
    }

    @Test
    public void shouldCreateEventLoopGroup()
    {
        eventLoopGroup = EventLoopGroupFactory.newEventLoopGroup();
        assertThat( eventLoopGroup, instanceOf( NioEventLoopGroup.class ) );
    }

    @Test
    public void shouldAssertNotInEventLoopThread() throws Exception
    {
        eventLoopGroup = EventLoopGroupFactory.newEventLoopGroup( 1 );

        // current thread is not an event loop thread, assertion should not throw
        EventLoopGroupFactory.assertNotInEventLoopThread();

        // submit assertion to the event loop thread, it should fail there
        Future<?> assertFuture = eventLoopGroup.next().submit( EventLoopGroupFactory::assertNotInEventLoopThread );
        try
        {
            assertFuture.get( 30, SECONDS );
            fail( "Exception expected" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), is( blockingOperationInEventLoopError() ) );
        }
    }
}
