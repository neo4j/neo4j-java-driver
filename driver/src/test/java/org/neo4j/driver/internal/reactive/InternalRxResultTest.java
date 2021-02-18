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
package org.neo4j.driver.internal.reactive;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.cursor.RxResultCursorImpl;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.PullResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.internal.reactive.util.ListBasedPullHandler;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Predicate.isEqual;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.Values.values;

class InternalRxResultTest
{
    @Test
    void shouldInitCursorFuture()
    {
        // Given
        RxResultCursor cursor = mock( RxResultCursorImpl.class );
        InternalRxResult rxResult = newRxResult( cursor );

        // When
        CompletableFuture<RxResultCursor> cursorFuture = rxResult.initCursorFuture().toCompletableFuture();

        // Then
        assertTrue( cursorFuture.isDone() );
        assertThat( Futures.getNow( cursorFuture ), equalTo( cursor ) );
    }

    @Test
    void shouldInitCursorFutureWithFailedCursor()
    {
        // Given
        RuntimeException error = new RuntimeException( "Failed to obtain cursor probably due to connection problem" );
        InternalRxResult rxResult = newRxResult( error );

        // When
        CompletableFuture<RxResultCursor> cursorFuture = rxResult.initCursorFuture().toCompletableFuture();

        // Then
        assertTrue( cursorFuture.isDone() );
        RuntimeException actualError = assertThrows( RuntimeException.class, () -> Futures.getNow( cursorFuture ) );
        assertThat( actualError.getCause(), equalTo( error ) );
    }

    @Test
    void shouldObtainKeys()
    {
        // Given
        RxResultCursor cursor = mock( RxResultCursorImpl.class );
        RxResult rxResult = newRxResult( cursor );

        List<String> keys = Arrays.asList( "one", "two", "three" );
        when( cursor.keys() ).thenReturn( keys );

        // When & Then
        StepVerifier.create( Flux.from( rxResult.keys() ) )
                .expectNext( Arrays.asList( "one", "two", "three" ) )
                .verifyComplete();
    }

    @Test
    void shouldErrorWhenFailedObtainKeys()
    {
        // Given
        RuntimeException error = new RuntimeException( "Failed to obtain cursor" );
        InternalRxResult rxResult = newRxResult( error );

        // When & Then
        StepVerifier.create( Flux.from( rxResult.keys() ) )
                .expectErrorMatches( isEqual( error ) )
                .verify();
    }

    @Test
    void shouldCancelKeys()
    {
        // Given
        RxResultCursor cursor = mock( RxResultCursorImpl.class );
        RxResult rxResult = newRxResult( cursor );

        List<String> keys = Arrays.asList( "one", "two", "three" );
        when( cursor.keys() ).thenReturn( keys );

        // When & Then
        StepVerifier.create( Flux.from( rxResult.keys() ).limitRate( 1 ).take( 1 ) )
                .expectNext( Arrays.asList( "one", "two", "three" ) )
                .verifyComplete();
    }

    @Test
    void shouldObtainRecordsAndSummary()
    {
        // Given
        Record record1 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 2, 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 3, 3 ) );

        PullResponseHandler pullHandler = new ListBasedPullHandler( Arrays.asList( record1, record2, record3 ) );
        RxResult rxResult = newRxResult( pullHandler );

        // When
        StepVerifier.create( Flux.from( rxResult.records() ) )
                .expectNext( record1 )
                .expectNext( record2 )
                .expectNext( record3 )
                .verifyComplete();
        StepVerifier.create( Mono.from( rxResult.consume() ) ).expectNextCount( 1 ).verifyComplete();
    }

    @Test
    void shouldCancelStreamingButObtainSummary()
    {
        // Given
        Record record1 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 2, 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 3, 3 ) );

        PullResponseHandler pullHandler = new ListBasedPullHandler( Arrays.asList( record1, record2, record3 ) );
        RxResult rxResult = newRxResult( pullHandler );

        // When
        StepVerifier.create( Flux.from( rxResult.records() ).limitRate( 1 ).take( 1 ) )
                .expectNext( record1 )
                .verifyComplete();
        StepVerifier.create( Mono.from( rxResult.consume() ) ).expectNextCount( 1 ).verifyComplete();
    }

    @Test
    void shouldErrorIfFailedToCreateCursor()
    {
        // Given
        Throwable error = new RuntimeException( "Hi" );
        RxResult rxResult = newRxResult( error );

        // When & Then
        StepVerifier.create( Flux.from( rxResult.records() ) ).expectErrorMatches( isEqual( error ) ).verify();
        StepVerifier.create( Mono.from( rxResult.consume() ) ).expectErrorMatches( isEqual( error ) ).verify();
    }

    @Test
    void shouldErrorIfFailedToStream()
    {
        // Given
        Throwable error = new RuntimeException( "Hi" );
        RxResult rxResult = newRxResult( new ListBasedPullHandler( error ) );

        // When & Then
        StepVerifier.create( Flux.from( rxResult.records() ) ).expectErrorMatches( isEqual( error ) ).verify();
        StepVerifier.create( Mono.from( rxResult.consume() ) ).assertNext( summary -> {
            assertThat( summary, instanceOf( ResultSummary.class ) );
        } ).verifyComplete();
    }

    private InternalRxResult newRxResult(PullResponseHandler pullHandler )
    {
        RunResponseHandler runHandler = mock( RunResponseHandler.class );
        when( runHandler.runFuture() ).thenReturn( Futures.completedWithNull() );
        RxResultCursor cursor = new RxResultCursorImpl( runHandler, pullHandler );
        return newRxResult( cursor );
    }

    private InternalRxResult newRxResult(RxResultCursor cursor )
    {
        return new InternalRxResult( () -> {
            // now we successfully run
            return completedFuture( cursor );
        } );
    }

    private InternalRxResult newRxResult(Throwable error )
    {
        return new InternalRxResult( () -> {
            // now we successfully run
            return failedFuture( new CompletionException( error ) );
        } );
    }
}
