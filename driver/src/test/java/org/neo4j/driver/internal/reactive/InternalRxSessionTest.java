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
package org.neo4j.driver.internal.reactive;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.reactive.InternalRxResult;
import org.neo4j.driver.internal.reactive.InternalRxSession;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.internal.reactive.cursor.RxStatementResultCursor;
import org.neo4j.driver.Statement;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.TransactionConfig.empty;
import static org.neo4j.driver.Values.parameters;

class InternalRxSessionTest
{
    private static Stream<Function<RxSession,RxResult>> allSessionRunMethods()
    {
        return Stream.of(
                rxSession -> rxSession.run( "RETURN 1" ),
                rxSession -> rxSession.run( "RETURN $x", parameters( "x", 1 ) ),
                rxSession -> rxSession.run( "RETURN $x", singletonMap( "x", 1 ) ),
                rxSession -> rxSession.run( "RETURN $x",
                        new InternalRecord( singletonList( "x" ), new Value[]{new IntegerValue( 1 )} ) ),
                rxSession -> rxSession.run( new Statement( "RETURN $x", parameters( "x", 1 ) ) ),
                rxSession -> rxSession.run( new Statement( "RETURN $x", parameters( "x", 1 ) ), empty() ),
                rxSession -> rxSession.run( "RETURN $x", singletonMap( "x", 1 ), empty() ),
                rxSession -> rxSession.run( "RETURN 1", empty() )
        );
    }

    private static Stream<Function<RxSession,Publisher<RxTransaction>>> allBeginTxMethods()
    {
        return Stream.of(
                rxSession -> rxSession.beginTransaction(),
                rxSession -> rxSession.beginTransaction( TransactionConfig.empty() )
        );
    }

    @ParameterizedTest
    @MethodSource( "allSessionRunMethods" )
    void shouldDelegateRun( Function<RxSession,RxResult> runReturnOne ) throws Throwable
    {
        // Given
        NetworkSession session = mock( NetworkSession.class );
        RxStatementResultCursor cursor = mock( RxStatementResultCursor.class );

        // Run succeeded with a cursor
        when( session.runRx( any( Statement.class ), any( TransactionConfig.class ) ) ).thenReturn( completedFuture( cursor ) );
        InternalRxSession rxSession = new InternalRxSession( session );

        // When
        RxResult result = runReturnOne.apply( rxSession );
        // Execute the run
        CompletionStage<RxStatementResultCursor> cursorFuture = ((InternalRxResult) result).cursorFutureSupplier().get();

        // Then
        verify( session ).runRx( any( Statement.class ), any( TransactionConfig.class ) );
        assertThat( Futures.getNow( cursorFuture ), equalTo( cursor ) );
    }

    @ParameterizedTest
    @MethodSource( "allSessionRunMethods" )
    void shouldReleaseConnectionIfFailedToRun( Function<RxSession,RxResult> runReturnOne ) throws Throwable
    {
        // Given
        Throwable error = new RuntimeException( "Hi there" );
        NetworkSession session = mock( NetworkSession.class );

        // Run failed with error
        when( session.runRx( any( Statement.class ), any( TransactionConfig.class ) ) ).thenReturn( Futures.failedFuture( error ) );
        when( session.releaseConnection() ).thenReturn( Futures.completedWithNull() );

        InternalRxSession rxSession = new InternalRxSession( session );

        // When
        RxResult result = runReturnOne.apply( rxSession );
        // Execute the run
        CompletionStage<RxStatementResultCursor> cursorFuture = ((InternalRxResult) result).cursorFutureSupplier().get();

        // Then
        verify( session ).runRx( any( Statement.class ), any( TransactionConfig.class ) );
        RuntimeException t = assertThrows( CompletionException.class, () -> Futures.getNow( cursorFuture ) );
        assertThat( t.getCause(), equalTo( error ) );
        verify( session ).releaseConnection();
    }

    @ParameterizedTest
    @MethodSource( "allBeginTxMethods" )
    void shouldDelegateBeginTx( Function<RxSession,Publisher<RxTransaction>> beginTx ) throws Throwable
    {
        // Given
        NetworkSession session = mock( NetworkSession.class );
        ExplicitTransaction tx = mock( ExplicitTransaction.class );

        when( session.beginTransactionAsync( any( TransactionConfig.class ) ) ).thenReturn( completedFuture( tx ) );
        InternalRxSession rxSession = new InternalRxSession( session );

        // When
        Publisher<RxTransaction> rxTx = beginTx.apply( rxSession );
        StepVerifier.create( Mono.from( rxTx ) ).expectNextCount( 1 ).verifyComplete();

        // Then
        verify( session ).beginTransactionAsync( any( TransactionConfig.class ) );
    }

    @ParameterizedTest
    @MethodSource( "allBeginTxMethods" )
    void shouldReleaseConnectionIfFailedToBeginTx( Function<RxSession,Publisher<RxTransaction>> beginTx ) throws Throwable
    {
        // Given
        Throwable error = new RuntimeException( "Hi there" );
        NetworkSession session = mock( NetworkSession.class );

        // Run failed with error
        when( session.beginTransactionAsync( any( TransactionConfig.class ) ) ).thenReturn( Futures.failedFuture( error ) );
        when( session.releaseConnection() ).thenReturn( Futures.completedWithNull() );

        InternalRxSession rxSession = new InternalRxSession( session );

        // When
        Publisher<RxTransaction> rxTx = beginTx.apply( rxSession );
        CompletableFuture<RxTransaction> txFuture = Mono.from( rxTx ).toFuture();

        // Then
        verify( session ).beginTransactionAsync( any( TransactionConfig.class ) );
        RuntimeException t = assertThrows( CompletionException.class, () -> Futures.getNow( txFuture ) );
        assertThat( t.getCause(), equalTo( error ) );
        verify( session ).releaseConnection();
    }

    @Test
    void shouldDelegateBookmark() throws Throwable
    {
        // Given
        NetworkSession session = mock( NetworkSession.class );
        InternalRxSession rxSession = new InternalRxSession( session );

        // When
        rxSession.lastBookmark();

        // Then
        verify( session ).lastBookmark();
        verifyNoMoreInteractions( session );
    }

    @Test
    void shouldDelegateReset() throws Throwable
    {
        // Given
        NetworkSession session = mock( NetworkSession.class );
        when( session.resetAsync() ).thenReturn( completedWithNull() );
        InternalRxSession rxSession = new InternalRxSession( session );

        // When
        Publisher<Void> mono = rxSession.reset();

        // Then
        StepVerifier.create( mono ).verifyComplete();
        verify( session ).resetAsync();
        verifyNoMoreInteractions( session );
    }

    @Test
    void shouldDelegateClose() throws Throwable
    {
        // Given
        NetworkSession session = mock( NetworkSession.class );
        when( session.closeAsync() ).thenReturn( completedWithNull() );
        InternalRxSession rxSession = new InternalRxSession( session );

        // When
        Publisher<Void> mono = rxSession.close();

        // Then
        StepVerifier.create( mono ).verifyComplete();
        verify( session ).closeAsync();
        verifyNoMoreInteractions( session );
    }
}
