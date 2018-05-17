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

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.v1.Values.values;

public class ResetResponseHandlerTest
{
    @Test
    public void shouldCompleteFutureOnSuccess() throws Exception
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ResetResponseHandler handler = newHandler( future );

        assertFalse( future.isDone() );

        handler.onSuccess( emptyMap() );

        assertTrue( future.isDone() );
        assertNull( future.get() );
    }

    @Test
    public void shouldUnMuteAckFailureOnSuccess()
    {
        InboundMessageDispatcher messageDispatcher = mock( InboundMessageDispatcher.class );
        ResetResponseHandler handler = newHandler( messageDispatcher, new CompletableFuture<>() );

        handler.onSuccess( emptyMap() );

        verify( messageDispatcher ).unMuteAckFailure();
    }

    @Test
    public void shouldCompleteFutureOnFailure() throws Exception
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ResetResponseHandler handler = newHandler( future );

        assertFalse( future.isDone() );

        handler.onFailure( new RuntimeException() );

        assertTrue( future.isDone() );
        assertNull( future.get() );
    }

    @Test
    public void shouldUnMuteAckFailureOnFailure()
    {
        InboundMessageDispatcher messageDispatcher = mock( InboundMessageDispatcher.class );
        ResetResponseHandler handler = newHandler( messageDispatcher, new CompletableFuture<>() );

        handler.onFailure( new RuntimeException() );

        verify( messageDispatcher ).unMuteAckFailure();
    }

    @Test
    public void shouldThrowWhenOnRecord()
    {
        ResetResponseHandler handler = newHandler( new CompletableFuture<>() );

        try
        {
            handler.onRecord( values( 1, 2, 3 ) );
            fail( "Exception expected" );
        }
        catch ( UnsupportedOperationException ignore )
        {
        }
    }

    private static ResetResponseHandler newHandler( CompletableFuture<Void> future )
    {
        return new ResetResponseHandler( mock( InboundMessageDispatcher.class ), future );
    }

    private static ResetResponseHandler newHandler( InboundMessageDispatcher messageDispatcher,
            CompletableFuture<Void> future )
    {
        return new ResetResponseHandler( messageDispatcher, future );
    }
}
