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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Values.values;

class ResetResponseHandlerTest
{
    @Test
    void shouldCompleteFutureOnSuccess() throws Exception
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ResetResponseHandler handler = newHandler( future );

        assertFalse( future.isDone() );

        handler.onSuccess( emptyMap() );

        assertTrue( future.isDone() );
        assertNull( future.get() );
    }

    @Test
    void shouldCompleteFutureOnFailure() throws Exception
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ResetResponseHandler handler = newHandler( future );

        assertFalse( future.isDone() );

        handler.onFailure( new RuntimeException() );

        assertTrue( future.isDone() );
        assertNull( future.get() );
    }

    @Test
    void shouldThrowWhenOnRecord()
    {
        ResetResponseHandler handler = newHandler( new CompletableFuture<>() );

        assertThrows( UnsupportedOperationException.class, () -> handler.onRecord( values( 1, 2, 3 ) ) );
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
