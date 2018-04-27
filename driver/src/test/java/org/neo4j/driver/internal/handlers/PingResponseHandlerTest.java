/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import io.netty.channel.Channel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.Test;

import org.neo4j.driver.v1.Value;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;

public class PingResponseHandlerTest
{
    @Test
    public void shouldResolvePromiseOnSuccess()
    {
        Promise<Boolean> promise = newPromise();
        PingResponseHandler handler = newHandler( promise );

        handler.onSuccess( emptyMap() );

        assertTrue( promise.isSuccess() );
        assertTrue( promise.getNow() );
    }

    @Test
    public void shouldResolvePromiseOnFailure()
    {
        Promise<Boolean> promise = newPromise();
        PingResponseHandler handler = newHandler( promise );

        handler.onFailure( new RuntimeException() );

        assertTrue( promise.isSuccess() );
        assertFalse( promise.getNow() );
    }

    @Test
    public void shouldNotSupportRecordMessages()
    {
        PingResponseHandler handler = newHandler( newPromise() );

        try
        {
            handler.onRecord( new Value[0] );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
    }

    private static Promise<Boolean> newPromise()
    {
        return ImmediateEventExecutor.INSTANCE.newPromise();
    }

    private static PingResponseHandler newHandler( Promise<Boolean> result )
    {
        return new PingResponseHandler( result, mock( Channel.class ), DEV_NULL_LOGGER );
    }
}
