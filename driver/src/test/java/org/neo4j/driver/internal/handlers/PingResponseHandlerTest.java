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

import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.driver.v1.Value;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PingResponseHandlerTest
{
    @Test
    public void shouldResolvePromiseOnSuccess()
    {
        Promise<Boolean> promise = newPromise();
        PingResponseHandler handler = new PingResponseHandler( promise );

        handler.onSuccess( Collections.<String,Value>emptyMap() );

        assertTrue( promise.isSuccess() );
        assertTrue( promise.getNow() );
    }

    @Test
    public void shouldResolvePromiseOnFailure()
    {
        Promise<Boolean> promise = newPromise();
        PingResponseHandler handler = new PingResponseHandler( promise );

        handler.onFailure( new RuntimeException() );

        assertTrue( promise.isSuccess() );
        assertFalse( promise.getNow() );
    }

    @Test
    public void shouldNotSupportRecordMessages()
    {
        PingResponseHandler handler = new PingResponseHandler( newPromise() );

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
}
