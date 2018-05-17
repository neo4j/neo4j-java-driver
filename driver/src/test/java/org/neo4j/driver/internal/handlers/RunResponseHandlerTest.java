/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.v1.Values.value;
import static org.neo4j.driver.v1.Values.values;

public class RunResponseHandlerTest
{
    @Test
    public void shouldNotifyCompletionFutureOnSuccess() throws Exception
    {
        CompletableFuture<Void> runCompletedFuture = new CompletableFuture<>();
        RunResponseHandler handler = newHandler( runCompletedFuture );

        assertFalse( runCompletedFuture.isDone() );
        handler.onSuccess( emptyMap() );

        assertTrue( runCompletedFuture.isDone() );
        assertNull( runCompletedFuture.get() );
    }

    @Test
    public void shouldNotifyCompletionFutureOnFailure() throws Exception
    {
        CompletableFuture<Void> runCompletedFuture = new CompletableFuture<>();
        RunResponseHandler handler = newHandler( runCompletedFuture );

        assertFalse( runCompletedFuture.isDone() );
        handler.onFailure( new RuntimeException() );

        assertTrue( runCompletedFuture.isDone() );
        assertNull( runCompletedFuture.get() );
    }

    @Test
    public void shouldThrowOnRecord()
    {
        RunResponseHandler handler = newHandler();

        try
        {
            handler.onRecord( values( "a", "b", "c" ) );
            fail( "Exception expected" );
        }
        catch ( UnsupportedOperationException ignore )
        {
        }
    }

    @Test
    public void shouldReturnNoKeysWhenFailed()
    {
        RunResponseHandler handler = newHandler();

        handler.onFailure( new RuntimeException() );

        assertEquals( emptyList(), handler.statementKeys() );
    }

    @Test
    public void shouldReturnDefaultResultAvailableAfterWhenFailed()
    {
        RunResponseHandler handler = newHandler();

        handler.onFailure( new RuntimeException() );

        assertEquals( -1, handler.resultAvailableAfter() );
    }

    @Test
    public void shouldReturnKeysWhenSucceeded()
    {
        RunResponseHandler handler = newHandler();

        List<String> keys = asList( "key1", "key2", "key3" );
        handler.onSuccess( singletonMap( "fields", value( keys ) ) );

        assertEquals( keys, handler.statementKeys() );
    }

    @Test
    public void shouldReturnResultAvailableAfterWhenSucceeded()
    {
        RunResponseHandler handler = newHandler();

        handler.onSuccess( singletonMap( "result_available_after", value( 42 ) ) );

        assertEquals( 42L, handler.resultAvailableAfter() );
    }

    private static RunResponseHandler newHandler()
    {
        return new RunResponseHandler( new CompletableFuture<>() );
    }

    private static RunResponseHandler newHandler( CompletableFuture<Void> runCompletedFuture )
    {
        return new RunResponseHandler( runCompletedFuture );
    }
}
