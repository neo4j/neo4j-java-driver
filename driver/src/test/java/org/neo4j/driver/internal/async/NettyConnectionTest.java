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
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.internal.ConcurrentSet;
import org.junit.After;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.FakeClock;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.messaging.ResetMessage.RESET;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.v1.util.DaemonThreadFactory.daemon;

public class NettyConnectionTest
{
    private static final NoOpResponseHandler NO_OP_HANDLER = NoOpResponseHandler.INSTANCE;

    private ExecutorService executor;
    private EventLoop eventLoop;

    @After
    public void tearDown() throws Exception
    {
        shutdownEventLoop();
    }

    @Test
    public void shouldBeOpenAfterCreated()
    {
        NettyConnection connection = newConnection( new EmbeddedChannel() );
        assertTrue( connection.isOpen() );
    }

    @Test
    public void shouldNotBeOpenAfterRelease()
    {
        NettyConnection connection = newConnection( new EmbeddedChannel() );
        connection.releaseNow();
        assertFalse( connection.isOpen() );
    }

    @Test
    public void shouldSendResetOnRelease()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        InboundMessageDispatcher messageDispatcher = new InboundMessageDispatcher( channel, DEV_NULL_LOGGING );
        ChannelAttributes.setMessageDispatcher( channel, messageDispatcher );

        NettyConnection connection = newConnection( channel );

        connection.releaseNow();
        channel.runPendingTasks();

        assertEquals( 1, channel.outboundMessages().size() );
        assertEquals( RESET, channel.readOutbound() );
    }

    @Test
    public void shouldEnqueueRunHandlerFromEventLoopThread() throws Exception
    {
        testWriteInEventLoop( "RunTestEventLoop",
                connection -> connection.run( "RETURN 1", emptyMap(), NO_OP_HANDLER, NO_OP_HANDLER ) );
    }

    @Test
    public void shouldWriteRunAndFlushInEventLoopThread() throws Exception
    {
        testWriteInEventLoop( "RunAndFlushTestEventLoop",
                connection -> connection.runAndFlush( "RETURN 1", emptyMap(), NO_OP_HANDLER, NO_OP_HANDLER ) );
    }

    @Test
    public void shouldWriteReleaseInEventLoopThread() throws Exception
    {
        testWriteInEventLoop( "ReleaseTestEventLoop", NettyConnection::releaseInBackground );
    }

    @Test
    public void shouldWriteForceReleaseInEventLoopThread() throws Exception
    {
        testWriteInEventLoop( "ReleaseNowTestEventLoop", NettyConnection::releaseNow );
    }

    private void testWriteInEventLoop( String threadName, Consumer<NettyConnection> action ) throws Exception
    {
        EmbeddedChannel channel = spy( new EmbeddedChannel() );
        initializeEventLoop( channel, threadName );
        ThreadTrackingInboundMessageDispatcher dispatcher = new ThreadTrackingInboundMessageDispatcher( channel );
        setMessageDispatcher( channel, dispatcher );

        NettyConnection connection = newConnection( channel );
        action.accept( connection );

        shutdownEventLoop();
        assertThat( single( dispatcher.queueThreadNames ), startsWith( threadName ) );
    }

    private void initializeEventLoop( Channel channel, String namePrefix )
    {
        executor = Executors.newSingleThreadExecutor( daemon( namePrefix ) );
        eventLoop = new DefaultEventLoop( executor );
        when( channel.eventLoop() ).thenReturn( eventLoop );
    }

    private void shutdownEventLoop() throws Exception
    {
        if ( eventLoop != null )
        {
            eventLoop.shutdownGracefully();
        }
        if ( executor != null )
        {
            executor.shutdown();
            assertTrue( executor.awaitTermination( 30, TimeUnit.SECONDS ) );
        }
    }

    private static NettyConnection newConnection( EmbeddedChannel channel )
    {
        return new NettyConnection( channel, mock( ChannelPool.class ), new FakeClock() );
    }

    private static class ThreadTrackingInboundMessageDispatcher extends InboundMessageDispatcher
    {
        final Set<String> queueThreadNames = new ConcurrentSet<>();

        ThreadTrackingInboundMessageDispatcher( Channel channel )
        {
            super( channel, DEV_NULL_LOGGING );
        }

        @Override
        public void queue( ResponseHandler handler )
        {
            queueThreadNames.add( Thread.currentThread().getName() );
            super.queue( handler );
        }
    }
}
