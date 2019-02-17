/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.react.internal.cursor;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.util.Futures;

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.v1.Values.value;

class RxStatementResultCursorTest
{
    @Test
    void shouldReturnStatementKeys() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        List<String> expected = asList( "key1", "key2", "key3" );
        runHandler.onSuccess( Collections.singletonMap( "fields", value( expected ) ) );

        BasicPullResponseHandler pullHandler = mock( BasicPullResponseHandler.class );

        // When
        RxStatementResultCursor cursor = new RxStatementResultCursor( runHandler, pullHandler );
        List<String> actual = cursor.keys();

        // Then
        assertEquals( expected, actual );
    }

    @Test
    void runErrorShouldFailPullHandlerWhenRequest() throws Throwable
    {
        // Given
        RuntimeException error = new RuntimeException( "Hi" );
        CompletableFuture<Throwable> runFuture = completedFuture( error );
        RunResponseHandler runHandler = newRunResponseHandler( runFuture );

        BasicPullResponseHandler pullHandler = mock( BasicPullResponseHandler.class );
        RxStatementResultCursor cursor = new RxStatementResultCursor( error, runHandler, pullHandler );

        // When
        cursor.request( 100 );

        // Then
        verify( pullHandler ).onFailure( error );
    }

    @Test
    void runErrorShouldFailPullHandlerWhenCancel() throws Throwable
    {
        // Given
        RuntimeException error = new RuntimeException( "Hi" );
        CompletableFuture<Throwable> runFuture = completedFuture( error );
        RunResponseHandler runHandler = newRunResponseHandler( runFuture );

        BasicPullResponseHandler pullHandler = mock( BasicPullResponseHandler.class );
        RxStatementResultCursor cursor = new RxStatementResultCursor( error, runHandler, pullHandler );

        // When
        cursor.cancel();

        // Then
        verify( pullHandler ).onFailure( error );
    }

    private static RunResponseHandler newRunResponseHandler( CompletableFuture<Throwable> runCompletedFuture )
    {
        return new RunResponseHandler( runCompletedFuture, BoltProtocolV4.METADATA_EXTRACTOR );
    }

    private static RunResponseHandler newRunResponseHandler()
    {
        return new RunResponseHandler( Futures.completedWithNull(), BoltProtocolV4.METADATA_EXTRACTOR );
    }
}
