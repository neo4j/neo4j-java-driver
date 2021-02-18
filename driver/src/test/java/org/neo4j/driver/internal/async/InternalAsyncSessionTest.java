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
package org.neo4j.driver.internal.async;

import org.hamcrest.junit.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.AsyncTransactionWork;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.neo4j.driver.internal.value.IntegerValue;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.TransactionConfig.empty;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.connectionMock;
import static org.neo4j.driver.util.TestUtil.newSession;
import static org.neo4j.driver.util.TestUtil.setupFailingCommit;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.util.TestUtil.verifyBeginTx;
import static org.neo4j.driver.util.TestUtil.verifyCommitTx;
import static org.neo4j.driver.util.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.util.TestUtil.verifyRunAndPull;

class InternalAsyncSessionTest
{
    private Connection connection;
    private ConnectionProvider connectionProvider;
    private AsyncSession asyncSession;
    private NetworkSession session;

    @BeforeEach
    void setUp()
    {
        connection = connectionMock( BoltProtocolV4.INSTANCE );
        connectionProvider = mock( ConnectionProvider.class );
        when( connectionProvider.acquireConnection( any( ConnectionContext.class ) ) )
                .thenReturn( completedFuture( connection ) );
        session = newSession( connectionProvider );
        asyncSession = new InternalAsyncSession( session );
    }

    private static Stream<Function<AsyncSession,CompletionStage<ResultCursor>>> allSessionRunMethods()
    {
        return Stream.of(
                session -> session.runAsync( "RETURN 1" ),
                session -> session.runAsync( "RETURN $x", parameters( "x", 1 ) ),
                session -> session.runAsync( "RETURN $x", singletonMap( "x", 1 ) ),
                session -> session.runAsync( "RETURN $x",
                        new InternalRecord( singletonList( "x" ), new Value[]{new IntegerValue( 1 )} ) ),
                session -> session.runAsync( new Query( "RETURN $x", parameters( "x", 1 ) ) ),
                session -> session.runAsync( new Query( "RETURN $x", parameters( "x", 1 ) ), empty() ),
                session -> session.runAsync( "RETURN $x", singletonMap( "x", 1 ), empty() ),
                session -> session.runAsync( "RETURN 1", empty() )
        );
    }

    private static Stream<Function<AsyncSession,CompletionStage<AsyncTransaction>>> allBeginTxMethods()
    {
        return Stream.of(
                session -> session.beginTransactionAsync(),
                session -> session.beginTransactionAsync( TransactionConfig.empty() )
        );
    }

    private static Stream<Function<AsyncSession, CompletionStage<String>>> allRunTxMethods()
    {
        return Stream.of(
                session -> session.readTransactionAsync( tx -> completedFuture( "a" ) ),
                session -> session.writeTransactionAsync( tx -> completedFuture( "a" ) ),
                session -> session.readTransactionAsync( tx -> completedFuture( "a" ), empty() ),
                session -> session.writeTransactionAsync( tx -> completedFuture( "a" ), empty() )
        );
    }

    @ParameterizedTest
    @MethodSource( "allSessionRunMethods" )
    void shouldFlushOnRun( Function<AsyncSession,CompletionStage<ResultCursor>> runReturnOne ) throws Throwable
    {
        setupSuccessfulRunAndPull( connection );

        ResultCursor cursor = await( runReturnOne.apply( asyncSession ) );

        verifyRunAndPull( connection, await( cursor.consumeAsync() ).query().text() );
    }

    @ParameterizedTest
    @MethodSource( "allBeginTxMethods" )
    void shouldDelegateBeginTx( Function<AsyncSession,CompletionStage<AsyncTransaction>> beginTx ) throws Throwable
    {
        AsyncTransaction tx = await( beginTx.apply( asyncSession ) );

        verifyBeginTx( connection );
        assertNotNull( tx );
    }

    @ParameterizedTest
    @MethodSource( "allRunTxMethods" )
    void txRunShouldBeginAndCommitTx( Function<AsyncSession,CompletionStage<String>> runTx ) throws Throwable
    {
        String string = await( runTx.apply( asyncSession ) );

        verifyBeginTx( connection );
        verifyCommitTx( connection );
        verify( connection ).release();
        assertThat( string, equalTo( "a" ) );
    }


    @Test
    void rollsBackReadTxWhenFunctionThrows()
    {
        testTxRollbackWhenThrows( READ );
    }

    @Test
    void rollsBackWriteTxWhenFunctionThrows()
    {
        testTxRollbackWhenThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilSuccessWhenFunctionThrows()
    {
        testTxIsRetriedUntilSuccessWhenFunctionThrows( READ );
    }

    @Test
    void writeTxRetriedUntilSuccessWhenFunctionThrows()
    {
        testTxIsRetriedUntilSuccessWhenFunctionThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilSuccessWhenTxCloseThrows()
    {
        testTxIsRetriedUntilSuccessWhenCommitThrows( READ );
    }

    @Test
    void writeTxRetriedUntilSuccessWhenTxCloseThrows()
    {
        testTxIsRetriedUntilSuccessWhenCommitThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilFailureWhenFunctionThrows()
    {
        testTxIsRetriedUntilFailureWhenFunctionThrows( READ );
    }

    @Test
    void writeTxRetriedUntilFailureWhenFunctionThrows()
    {
        testTxIsRetriedUntilFailureWhenFunctionThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilFailureWhenTxCloseThrows()
    {
        testTxIsRetriedUntilFailureWhenCommitFails( READ );
    }

    @Test
    void writeTxRetriedUntilFailureWhenTxCloseThrows()
    {
        testTxIsRetriedUntilFailureWhenCommitFails( WRITE );
    }


    @Test
    void shouldCloseSession() throws Throwable
    {
        await ( asyncSession.closeAsync() );
        assertFalse( this.session.isOpen() );
    }

    @Test
    void shouldReturnBookmark() throws Throwable
    {
        session = newSession( connectionProvider, InternalBookmark.parse( "Bookmark1" ) );
        asyncSession = new InternalAsyncSession( session );

        assertThat( asyncSession.lastBookmark(), equalTo( session.lastBookmark() ));
    }

    private void testTxRollbackWhenThrows( AccessMode transactionMode )
    {
        final RuntimeException error = new IllegalStateException( "Oh!" );
        AsyncTransactionWork<CompletionStage<Void>> work = tx ->
        {
            throw error;
        };

        Exception e = assertThrows( Exception.class, () -> executeTransaction( asyncSession, transactionMode, work ) );
        assertEquals( error, e );

        verify( connectionProvider ).acquireConnection( any( ConnectionContext.class ) );
        verifyBeginTx( connection );
        verifyRollbackTx( connection );
    }

    private void testTxIsRetriedUntilSuccessWhenFunctionThrows( AccessMode mode )
    {
        int failures = 12;
        int retries = failures + 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        session = newSession( connectionProvider, retryLogic );
        asyncSession = new InternalAsyncSession( session );

        TxWork work = spy( new TxWork( 42, failures, new SessionExpiredException( "" ) ) );
        int answer = executeTransaction( asyncSession, mode, work );

        assertEquals( 42, answer );
        verifyInvocationCount( work, failures + 1 );
        verifyCommitTx( connection );
        verifyRollbackTx( connection, times( failures ) );
    }

    private void testTxIsRetriedUntilSuccessWhenCommitThrows( AccessMode mode )
    {
        int failures = 13;
        int retries = failures + 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        setupFailingCommit( connection, failures );
        session = newSession( connectionProvider, retryLogic );
        asyncSession = new InternalAsyncSession( session );

        TxWork work = spy( new TxWork( 43 ) );
        int answer = executeTransaction( asyncSession, mode, work );

        assertEquals( 43, answer );
        verifyInvocationCount( work, failures + 1 );
        verifyCommitTx( connection, times( retries ) );
    }

    private void testTxIsRetriedUntilFailureWhenFunctionThrows( AccessMode mode )
    {
        int failures = 14;
        int retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        session = newSession( connectionProvider, retryLogic );
        asyncSession = new InternalAsyncSession( session );

        TxWork work = spy( new TxWork( 42, failures, new SessionExpiredException( "Oh!" ) ) );

        Exception e = assertThrows( Exception.class, () -> executeTransaction( asyncSession, mode, work ) );

        MatcherAssert.assertThat( e, instanceOf( SessionExpiredException.class ) );
        assertEquals( "Oh!", e.getMessage() );
        verifyInvocationCount( work, failures );
        verifyCommitTx( connection, never() );
        verifyRollbackTx( connection, times( failures ) );
    }

    private void testTxIsRetriedUntilFailureWhenCommitFails( AccessMode mode )
    {
        int failures = 17;
        int retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        setupFailingCommit( connection, failures );
        session = newSession( connectionProvider, retryLogic );
        asyncSession = new InternalAsyncSession( session );

        TxWork work = spy( new TxWork( 42 ) );

        Exception e = assertThrows( Exception.class, () -> executeTransaction( asyncSession, mode, work ) );

        MatcherAssert.assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        verifyInvocationCount( work, failures );
        verifyCommitTx( connection, times( failures ) );
    }

    private static <T> T executeTransaction( AsyncSession session, AccessMode mode, AsyncTransactionWork<CompletionStage<T>> work )
    {
        if ( mode == READ )
        {
            return await( session.readTransactionAsync( work ) );
        }
        else if ( mode == WRITE )
        {
            return await( session.writeTransactionAsync( work ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown mode " + mode );
        }
    }

    private static void verifyInvocationCount( AsyncTransactionWork<?> workSpy, int expectedInvocationCount )
    {
        verify( workSpy, times( expectedInvocationCount ) ).execute( any( AsyncTransaction.class ) );
    }

    private static class TxWork implements AsyncTransactionWork<CompletionStage<Integer>>
    {
        final int result;
        final int timesToThrow;
        final Supplier<RuntimeException> errorSupplier;

        int invoked;

        @SuppressWarnings( "unchecked" )
        TxWork( int result )
        {
            this( result, 0, (Supplier) null );
        }

        TxWork( int result, int timesToThrow, final RuntimeException error )
        {
            this.result = result;
            this.timesToThrow = timesToThrow;
            this.errorSupplier = () -> error;
        }

        TxWork( int result, int timesToThrow, Supplier<RuntimeException> errorSupplier )
        {
            this.result = result;
            this.timesToThrow = timesToThrow;
            this.errorSupplier = errorSupplier;
        }

        @Override
        public CompletionStage<Integer> execute( AsyncTransaction tx )
        {
            if ( timesToThrow > 0 && invoked++ < timesToThrow )
            {
                throw errorSupplier.get();
            }
            return completedFuture( result );
        }
    }
}
