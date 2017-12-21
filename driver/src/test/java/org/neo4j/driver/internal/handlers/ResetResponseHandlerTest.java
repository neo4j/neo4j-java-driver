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
package org.neo4j.driver.internal.handlers;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.FakeClock;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.ChannelAttributes.lastUsedTimestamp;

public class ResetResponseHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher messageDispatcher = mock( InboundMessageDispatcher.class );

    @After
    public void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldReleaseChannelOnSuccess()
    {
        ChannelPool pool = newChannelPoolMock();
        FakeClock clock = new FakeClock();
        clock.progress( 5 );
        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        ResetResponseHandler handler = newHandler( pool, clock, releaseFuture );

        handler.onSuccess( emptyMap() );

        verifyLastUsedTimestamp( 5 );
        verify( pool ).release( eq( channel ) );
        assertTrue( releaseFuture.isDone() );
        assertFalse( releaseFuture.isCompletedExceptionally() );
    }

    @Test
    public void shouldReleaseChannelOnFailure()
    {
        ChannelPool pool = newChannelPoolMock();
        FakeClock clock = new FakeClock();
        clock.progress( 100 );
        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        ResetResponseHandler handler = newHandler( pool, clock, releaseFuture );

        handler.onFailure( new RuntimeException() );

        verifyLastUsedTimestamp( 100 );
        verify( pool ).release( eq( channel ) );
        assertTrue( releaseFuture.isDone() );
        assertFalse( releaseFuture.isCompletedExceptionally() );
    }

    @Test
    public void shouldUnMuteAckFailureOnSuccess()
    {
        ChannelPool pool = newChannelPoolMock();
        ResetResponseHandler handler = newHandler( pool, new FakeClock(), new CompletableFuture<>() );

        handler.onSuccess( emptyMap() );

        verify( messageDispatcher ).unMuteAckFailure();
    }

    @Test
    public void shouldUnMuteAckFailureOnFailure()
    {
        ChannelPool pool = newChannelPoolMock();
        ResetResponseHandler handler = newHandler( pool, new FakeClock(), new CompletableFuture<>() );

        handler.onFailure( new RuntimeException() );

        verify( messageDispatcher ).unMuteAckFailure();
    }

    private void verifyLastUsedTimestamp( int expectedValue )
    {
        assertEquals( expectedValue, lastUsedTimestamp( channel ).intValue() );
    }

    private ResetResponseHandler newHandler( ChannelPool pool, Clock clock, CompletableFuture<Void> releaseFuture )
    {
        return new ResetResponseHandler( channel, pool, messageDispatcher, clock, releaseFuture );
    }

    private static ChannelPool newChannelPoolMock()
    {
        ChannelPool pool = mock( ChannelPool.class );
        Future<Void> releasedFuture = ImmediateEventExecutor.INSTANCE.newSucceededFuture( null );
        when( pool.release( any() ) ).thenReturn( releasedFuture );
        return pool;
    }
}
