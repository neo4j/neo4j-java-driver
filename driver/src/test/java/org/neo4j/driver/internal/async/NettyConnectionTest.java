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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.ServerVersion;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.ChannelAttributes.terminationReason;
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
        connection.release();
        assertFalse( connection.isOpen() );
    }

    @Test
    public void shouldSendResetOnRelease()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        InboundMessageDispatcher messageDispatcher = new InboundMessageDispatcher( channel, DEV_NULL_LOGGING );
        ChannelAttributes.setMessageDispatcher( channel, messageDispatcher );

        NettyConnection connection = newConnection( channel );

        connection.release();
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
    public void shouldWriteForceReleaseInEventLoopThread() throws Exception
    {
        testWriteInEventLoop( "ReleaseTestEventLoop", NettyConnection::release );
    }

    @Test
    public void shouldEnableAutoReadWhenReleased()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.config().setAutoRead( false );

        NettyConnection connection = newConnection( channel );

        connection.release();
        assertTrue( channel.config().isAutoRead() );
    }

    @Test
    public void shouldNotDisableAutoReadWhenReleased()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.config().setAutoRead( true );

        NettyConnection connection = newConnection( channel );

        connection.release();
        connection.disableAutoRead(); // does nothing on released connection
        assertTrue( channel.config().isAutoRead() );
    }

    @Test
    public void shouldNotRunWhenReleased()
    {
        NettyConnection connection = newConnection( new EmbeddedChannel() );

        connection.release();

        try
        {
            connection.run( "RETURN 1", emptyMap(), mock( ResponseHandler.class ), mock( ResponseHandler.class ) );
            fail( "Exception expected" );
        }
        catch ( IllegalStateException e )
        {
            assertConnectionReleasedError( e );
        }
    }

    @Test
    public void shouldNotRunAndFlushWhenReleased()
    {
        NettyConnection connection = newConnection( new EmbeddedChannel() );

        connection.release();

        try
        {
            connection.runAndFlush( "RETURN 1", emptyMap(), mock( ResponseHandler.class ),
                    mock( ResponseHandler.class ) );
            fail( "Exception expected" );
        }
        catch ( IllegalStateException e )
        {
            assertConnectionReleasedError( e );
        }
    }

    @Test
    public void shouldReturnServerAddressWhenReleased()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        BoltServerAddress address = new BoltServerAddress( "host", 4242 );
        ChannelAttributes.setServerAddress( channel, address );

        NettyConnection connection = newConnection( channel );
        connection.release();

        assertEquals( address, connection.serverAddress() );
    }

    @Test
    public void shouldReturnServerVersionWhenReleased()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        ServerVersion version = ServerVersion.v3_2_0;
        ChannelAttributes.setServerVersion( channel, version );

        NettyConnection connection = newConnection( channel );
        connection.release();

        assertEquals( version, connection.serverVersion() );
    }

    @Test
    public void shouldReturnSameCompletionStageFromRelease()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        InboundMessageDispatcher messageDispatcher = new InboundMessageDispatcher( channel, DEV_NULL_LOGGING );
        ChannelAttributes.setMessageDispatcher( channel, messageDispatcher );

        NettyConnection connection = newConnection( channel );

        CompletionStage<Void> releaseStage1 = connection.release();
        CompletionStage<Void> releaseStage2 = connection.release();
        CompletionStage<Void> releaseStage3 = connection.release();

        channel.runPendingTasks();

        // RESET should be send only once
        assertEquals( 1, channel.outboundMessages().size() );
        assertEquals( RESET, channel.outboundMessages().poll() );

        // all returned stages should be the same
        assertEquals( releaseStage1, releaseStage2 );
        assertEquals( releaseStage2, releaseStage3 );
    }

    @Test
    public void shouldEnableAutoRead()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.config().setAutoRead( false );
        NettyConnection connection = newConnection( channel );

        connection.enableAutoRead();

        assertTrue( channel.config().isAutoRead() );
    }

    @Test
    public void shouldDisableAutoRead()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.config().setAutoRead( true );
        NettyConnection connection = newConnection( channel );

        connection.disableAutoRead();

        assertFalse( channel.config().isAutoRead() );
    }

    @Test
    public void shouldSetTerminationReasonOnChannelWhenTerminated()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        NettyConnection connection = newConnection( channel );

        String reason = "Something really bad has happened";
        connection.terminateAndRelease( reason );

        assertEquals( reason, terminationReason( channel ) );
    }

    @Test
    public void shouldCloseChannelWhenTerminated()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        NettyConnection connection = newConnection( channel );
        assertTrue( channel.isActive() );

        connection.terminateAndRelease( "test" );

        assertFalse( channel.isActive() );
    }

    @Test
    public void shouldReleaseChannelWhenTerminated()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelPool pool = mock( ChannelPool.class );
        NettyConnection connection = newConnection( channel, pool );
        verify( pool, never() ).release( any() );

        connection.terminateAndRelease( "test" );

        verify( pool ).release( channel );
    }

    @Test
    public void shouldNotReleaseChannelMultipleTimesWhenTerminatedMultipleTimes()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelPool pool = mock( ChannelPool.class );
        NettyConnection connection = newConnection( channel, pool );
        verify( pool, never() ).release( any() );

        connection.terminateAndRelease( "reason 1" );
        connection.terminateAndRelease( "reason 2" );
        connection.terminateAndRelease( "reason 3" );

        // channel is terminated with the first termination reason
        assertEquals( "reason 1", terminationReason( channel ) );
        // channel is released to the pool only once
        verify( pool ).release( channel );
    }

    @Test
    public void shouldNotReleaseAfterTermination()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelPool pool = mock( ChannelPool.class );
        NettyConnection connection = newConnection( channel, pool );
        verify( pool, never() ).release( any() );

        connection.terminateAndRelease( "test" );
        CompletionStage<Void> releaseStage = connection.release();

        // release stage should be completed immediately
        assertTrue( releaseStage.toCompletableFuture().isDone() );
        // channel is released to the pool only once
        verify( pool ).release( channel );
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

    private static NettyConnection newConnection( Channel channel )
    {
        return newConnection( channel, mock( ChannelPool.class ) );
    }

    private static NettyConnection newConnection( Channel channel, ChannelPool pool )
    {
        return new NettyConnection( channel, pool, new FakeClock() );
    }

    private static void assertConnectionReleasedError( IllegalStateException e )
    {
        assertThat( e.getMessage(), startsWith( "Connection has been released" ) );
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
