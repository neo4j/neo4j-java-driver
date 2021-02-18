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
package org.neo4j.driver.internal.handlers;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.pool.ExtendedChannelPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.FakeClock;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.lastUsedTimestamp;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

class ChannelReleasingResetResponseHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final InboundMessageDispatcher messageDispatcher = mock( InboundMessageDispatcher.class );

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldReleaseChannelOnSuccess()
    {
        ExtendedChannelPool pool = newChannelPoolMock();
        FakeClock clock = new FakeClock();
        clock.progress( 5 );
        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        ChannelReleasingResetResponseHandler handler = newHandler( pool, clock, releaseFuture );

        handler.onSuccess( emptyMap() );

        verifyLastUsedTimestamp( 5 );
        verify( pool ).release( eq( channel ) );
        assertTrue( releaseFuture.isDone() );
        assertFalse( releaseFuture.isCompletedExceptionally() );
    }

    @Test
    void shouldCloseAndReleaseChannelOnFailure()
    {
        ExtendedChannelPool pool = newChannelPoolMock();
        FakeClock clock = new FakeClock();
        clock.progress( 100 );
        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        ChannelReleasingResetResponseHandler handler = newHandler( pool, clock, releaseFuture );

        handler.onFailure( new RuntimeException() );

        assertTrue( channel.closeFuture().isDone() );
        verify( pool ).release( eq( channel ) );
        assertTrue( releaseFuture.isDone() );
        assertFalse( releaseFuture.isCompletedExceptionally() );
    }

    private void verifyLastUsedTimestamp( int expectedValue )
    {
        assertEquals( expectedValue, lastUsedTimestamp( channel ).intValue() );
    }

    private ChannelReleasingResetResponseHandler newHandler( ExtendedChannelPool pool, Clock clock,
            CompletableFuture<Void> releaseFuture )
    {
        return new ChannelReleasingResetResponseHandler( channel, pool, messageDispatcher, clock, releaseFuture );
    }

    private static ExtendedChannelPool newChannelPoolMock()
    {
        ExtendedChannelPool pool = mock( ExtendedChannelPool.class );
        when( pool.release( any() ) ).thenReturn( completedWithNull() );
        return pool;
    }
}
