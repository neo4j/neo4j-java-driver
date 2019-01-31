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
package org.neo4j.driver.internal.handlers.pulln;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.v1.Values.values;

class AsyncPullResponseHandlerTest
{
    private final Record record = new InternalRecord( asList( "key1", "key2" ), values( 1, 2 ) );

    @Test
    void shouldReturnNullRecordIfNoMoreRecord() throws Throwable
    {
        // Given
        BasicPullResponseHandler basicHandler = mock( BasicPullResponseHandler.class );
        when( basicHandler.isFinishedOrCanceled() ).thenReturn( true );
        AsyncPullResponseHandler asyncHandler = new AsyncPullResponseHandler( basicHandler );

        // When
        CompletableFuture<Record> future = asyncHandler.peekRecord();

        // Then
        assertTrue( future.isDone() );
        assertNull( future.get() );
    }

    @Test
    void shouldPeekNextRecordIfAlreadyArrived() throws Throwable
    {
        // Given
        AsyncPullResponseHandler asyncHandler = newAsyncPullResponseHandler();
        asyncHandler.queue().add( record );

        // When
        CompletableFuture<Record> future = asyncHandler.peekRecord();

        // Then
        assertTrue( future.isDone() );
        assertThat( future.get(), equalTo( record ) );
    }

    @Test
    void shouldPeekNextRecordWhenItArrives() throws Throwable
    {
        // Given
        AsyncPullResponseHandler asyncHandler = newAsyncPullResponseHandler();

        // When
        CompletableFuture<Record> peeked = asyncHandler.peekRecord();

        CompletableFuture<Record> peeked1 = asyncHandler.peekRecord();
        CompletableFuture<Record> peeked2 = asyncHandler.peekRecord();
        assertEquals( peeked, peeked1 );
        assertEquals( peeked, peeked2 );

        assertFalse( peeked.isDone() );

        // Then
        asyncHandler.consumeRecord( record, null );
        assertTrue( peeked.isDone() );
        assertThat( peeked.get(), equalTo( record ) );
    }

    @Test
    void shouldPeekNullIfNoMoreArrives() throws Throwable
    {
        // Given
        AsyncPullResponseHandler asyncHandler = newAsyncPullResponseHandler();

        // When
        CompletableFuture<Record> future = asyncHandler.peekRecord();
        assertFalse( future.isDone() );

        // Then
        asyncHandler.consumeRecord( null, null );
        assertTrue( future.isDone() );
        assertNull( future.get() );
    }

    @Test
    void shouldReturnSummary() throws Throwable
    {
        // Given
        AsyncPullResponseHandler asyncHandler = newAsyncPullResponseHandler();

        // When
        CompletableFuture<ResultSummary> summary = asyncHandler.summary();

        CompletableFuture<ResultSummary> summary1 = asyncHandler.summary();
        CompletableFuture<ResultSummary> summary2 = asyncHandler.summary();

        assertEquals( summary, summary1 );
        assertEquals( summary, summary2 );

        assertFalse( summary.isDone() );

        // Then
        ResultSummary mockedSummary = mock( ResultSummary.class );
        asyncHandler.consumeSummary( mockedSummary, null );
        assertTrue( summary.isDone() );
        assertThat( summary.get(), equalTo( mockedSummary ) );
    }

    private static AsyncPullResponseHandler newAsyncPullResponseHandler()
    {
        BasicPullResponseHandler basicHandler = mock( BasicPullResponseHandler.class );
        when( basicHandler.isFinishedOrCanceled() ).thenReturn( false );
        return new AsyncPullResponseHandler( basicHandler );
    }
}
