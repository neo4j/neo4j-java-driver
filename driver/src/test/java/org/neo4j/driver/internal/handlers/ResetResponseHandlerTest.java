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
import io.netty.util.concurrent.Promise;
import org.junit.After;
import org.junit.Test;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.FakeClock;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
        ChannelPool pool = mock( ChannelPool.class );
        FakeClock clock = new FakeClock();
        clock.progress( 5 );
        ResetResponseHandler handler = newHandler( pool, clock );

        handler.onSuccess( emptyMap() );

        verifyLastUsedTimestamp( 5 );
        verify( pool ).release( eq( channel ), any() );
    }

    @Test
    public void shouldReleaseChannelWithPromiseOnSuccess()
    {
        ChannelPool pool = mock( ChannelPool.class );
        FakeClock clock = new FakeClock();
        clock.progress( 42 );
        Promise<Void> promise = channel.newPromise();
        ResetResponseHandler handler = newHandler( pool, clock, promise );

        handler.onSuccess( emptyMap() );

        verifyLastUsedTimestamp( 42 );
        verify( pool ).release( channel, promise );
    }

    @Test
    public void shouldReleaseChannelOnFailure()
    {
        ChannelPool pool = mock( ChannelPool.class );
        FakeClock clock = new FakeClock();
        clock.progress( 100 );
        ResetResponseHandler handler = newHandler( pool, clock );

        handler.onFailure( new RuntimeException() );

        verifyLastUsedTimestamp( 100 );
        verify( pool ).release( eq( channel ), any() );
    }

    @Test
    public void shouldReleaseChannelWithPromiseOnFailure()
    {
        ChannelPool pool = mock( ChannelPool.class );
        FakeClock clock = new FakeClock();
        clock.progress( 99 );
        Promise<Void> promise = channel.newPromise();
        ResetResponseHandler handler = newHandler( pool, clock, promise );

        handler.onFailure( new RuntimeException() );

        verifyLastUsedTimestamp( 99 );
        verify( pool ).release( channel, promise );
    }

    @Test
    public void shouldUnMuteAckFailureOnSuccess()
    {
        ChannelPool pool = mock( ChannelPool.class );
        ResetResponseHandler handler = newHandler( pool, new FakeClock() );

        handler.onSuccess( emptyMap() );

        verify( messageDispatcher ).unMuteAckFailure();
    }

    @Test
    public void shouldUnMuteAckFailureOnFailure()
    {
        ChannelPool pool = mock( ChannelPool.class );
        ResetResponseHandler handler = newHandler( pool, new FakeClock() );

        handler.onFailure( new RuntimeException() );

        verify( messageDispatcher ).unMuteAckFailure();
    }

    private void verifyLastUsedTimestamp( int expectedValue )
    {
        assertEquals( expectedValue, lastUsedTimestamp( channel ).intValue() );
    }

    private ResetResponseHandler newHandler( ChannelPool pool, Clock clock )
    {
        return newHandler( pool, clock, channel.newPromise() );
    }

    private ResetResponseHandler newHandler( ChannelPool pool, Clock clock, Promise<Void> promise )
    {
        return new ResetResponseHandler( channel, pool, messageDispatcher, clock, promise );
    }
}
