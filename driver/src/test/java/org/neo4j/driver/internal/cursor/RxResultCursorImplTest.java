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
package org.neo4j.driver.internal.cursor;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.reactive.util.ListBasedPullHandler;

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.cursor.RxResultCursorImpl.DISCARD_RECORD_CONSUMER;
import static org.neo4j.driver.internal.messaging.v3.BoltProtocolV3.METADATA_EXTRACTOR;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

class RxResultCursorImplTest
{
    @Test
    void shouldWaitForRunToFinishBeforeCreatingRxResultCurosr() throws Throwable
    {
        // Given
        CompletableFuture<Throwable> runFuture = new CompletableFuture<>();
        RunResponseHandler runHandler = newRunResponseHandler( runFuture );
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        // When
        IllegalStateException error = assertThrows( IllegalStateException.class, () -> new RxResultCursorImpl( runHandler, pullHandler ) );
        // Then
        assertThat( error.getMessage(), containsString( "Should wait for response of RUN" ) );
    }

    @Test
    void shouldInstallSummaryConsumerWithoutReportingError() throws Throwable
    {
        // Given
        RuntimeException error = new RuntimeException( "Hi" );
        RunResponseHandler runHandler = newRunResponseHandler( error );
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        // When
        new RxResultCursorImpl( error, runHandler, pullHandler );

        // Then
        verify( pullHandler ).installSummaryConsumer( any( BiConsumer.class ) );
        verifyNoMoreInteractions( pullHandler );
    }

    @Test
    void shouldReturnQueryKeys() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        List<String> expected = asList( "key1", "key2", "key3" );
        runHandler.onSuccess( Collections.singletonMap( "fields", value( expected ) ) );

        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        // When
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );
        List<String> actual = cursor.keys();

        // Then
        assertEquals( expected, actual );
    }

    @Test
    void shouldSupportReturnQueryKeysMultipleTimes() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        List<String> expected = asList( "key1", "key2", "key3" );
        runHandler.onSuccess( Collections.singletonMap( "fields", value( expected ) ) );

        PullResponseHandler pullHandler = mock( PullResponseHandler.class );

        // When
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );

        // Then
        List<String> actual = cursor.keys();
        assertEquals( expected, actual );

        // Many times
        actual = cursor.keys();
        assertEquals( expected, actual );

        actual = cursor.keys();
        assertEquals( expected, actual );
    }

    @Test
    void shouldPull() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );

        // When
        cursor.request( 100 );

        // Then
        verify( pullHandler ).request( 100 );
    }

    @Test
    void shouldCancel() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );

        // When
        cursor.cancel();

        // Then
        verify( pullHandler ).cancel();
    }

    @Test
    void shouldInstallRecordConsumerAndReportError() throws Throwable
    {
        // Given
        RuntimeException error = new RuntimeException( "Hi" );
        BiConsumer recordConsumer = mock( BiConsumer.class );

        // When
        RunResponseHandler runHandler = newRunResponseHandler( error );
        PullResponseHandler pullHandler = new ListBasedPullHandler();
        RxResultCursor cursor = new RxResultCursorImpl( error, runHandler, pullHandler );
        cursor.installRecordConsumer( recordConsumer );

        // Then
        verify( recordConsumer ).accept( null, error );
        verifyNoMoreInteractions( recordConsumer );
    }

    @Test
    void shouldReturnSummaryFuture() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = new ListBasedPullHandler();
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );

        // When
        cursor.installRecordConsumer( DISCARD_RECORD_CONSUMER );
        cursor.request( 10 );
        cursor.summaryAsync();

        // Then
        assertTrue( cursor.isDone() );
    }

    @Test
    void shouldNotAllowToInstallRecordConsumerAfterSummary() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = new ListBasedPullHandler();
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );

        // When
        cursor.summaryAsync();

        // Then
        assertThrows( ResultConsumedException.class, () -> cursor.installRecordConsumer( null ) );
    }

    @Test
    void shouldAllowToCallSummaryMultipleTimes() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = new ListBasedPullHandler();
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );

        // When
        cursor.summaryAsync();

        // Then
        cursor.summaryAsync();
        cursor.summaryAsync();
    }

    @Test
    void shouldOnlyInstallRecordConsumerOnce() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );

        // When
        cursor.installRecordConsumer( DISCARD_RECORD_CONSUMER ); // any consumer
        cursor.installRecordConsumer( DISCARD_RECORD_CONSUMER ); // any consumer

        // Then
        verify( pullHandler ).installRecordConsumer( any() );
    }

    @Test
    void shouldCancelIfNotPulled() throws Throwable
    {
        // Given
        RunResponseHandler runHandler = newRunResponseHandler();
        PullResponseHandler pullHandler = mock( PullResponseHandler.class );
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );

        // When
        cursor.summaryAsync();

        // Then
        verify( pullHandler ).installRecordConsumer( DISCARD_RECORD_CONSUMER );
        verify( pullHandler ).cancel();
        assertFalse( cursor.isDone() );
    }

    private static RunResponseHandler newRunResponseHandler( CompletableFuture<Throwable> runFuture )
    {
        return new RunResponseHandler( runFuture, METADATA_EXTRACTOR );
    }

    private static RunResponseHandler newRunResponseHandler( Throwable error )
    {
        return newRunResponseHandler( completedFuture( error ) );
    }

    private static RunResponseHandler newRunResponseHandler()
    {
        return newRunResponseHandler( completedWithNull() );
    }
}
