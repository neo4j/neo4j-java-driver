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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Promise;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.Value;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.async.ChannelAttributes.lastUsedTimestamp;

public class ReleaseChannelHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @After
    public void tearDown() throws Exception
    {
        channel.close();
    }

    @Test
    public void shouldReleaseChannelOnSuccess()
    {
        ChannelPool pool = mock( ChannelPool.class );
        FakeClock clock = new FakeClock();
        clock.progress( 5 );
        ReleaseChannelHandler handler = new ReleaseChannelHandler( channel, pool, clock );

        handler.onSuccess( Collections.<String,Value>emptyMap() );

        verifyLastUsedTimestamp( 5 );
        verify( pool ).release( channel );
    }

    @Test
    public void shouldReleaseChannelWithPromiseOnSuccess()
    {
        ChannelPool pool = mock( ChannelPool.class );
        FakeClock clock = new FakeClock();
        clock.progress( 42 );
        Promise<Void> promise = channel.newPromise();
        ReleaseChannelHandler handler = new ReleaseChannelHandler( channel, pool, clock, promise );

        handler.onSuccess( Collections.<String,Value>emptyMap() );

        verifyLastUsedTimestamp( 42 );
        verify( pool ).release( channel, promise );
    }

    @Test
    public void shouldReleaseChannelOnFailure()
    {
        ChannelPool pool = mock( ChannelPool.class );
        FakeClock clock = new FakeClock();
        clock.progress( 100 );
        ReleaseChannelHandler handler = new ReleaseChannelHandler( channel, pool, clock );

        handler.onFailure( new RuntimeException() );

        verifyLastUsedTimestamp( 100 );
        verify( pool ).release( channel );
    }

    @Test
    public void shouldReleaseChannelWithPromiseOnFailure()
    {
        ChannelPool pool = mock( ChannelPool.class );
        FakeClock clock = new FakeClock();
        clock.progress( 99 );
        Promise<Void> promise = channel.newPromise();
        ReleaseChannelHandler handler = new ReleaseChannelHandler( channel, pool, clock, promise );

        handler.onFailure( new RuntimeException() );

        verifyLastUsedTimestamp( 99 );
        verify( pool ).release( channel, promise );
    }

    private void verifyLastUsedTimestamp( int expectedValue )
    {
        assertEquals( expectedValue, lastUsedTimestamp( channel ).intValue() );
    }
}
