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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Session;
import org.neo4j.driver.Statement;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.request.CommitMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.FixedRetryLogic;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
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
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.util.TestUtil.connectionMock;
import static org.neo4j.driver.util.TestUtil.newSession;
import static org.neo4j.driver.util.TestUtil.setupFailingBegin;
import static org.neo4j.driver.util.TestUtil.setupFailingCommit;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.util.TestUtil.verifyBeginTx;
import static org.neo4j.driver.util.TestUtil.verifyCommitTx;
import static org.neo4j.driver.util.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.util.TestUtil.verifyRunAndPull;

class InternalSessionTest
{
    private Connection connection;
    private ConnectionProvider connectionProvider;
    private InternalSession session;

    @BeforeEach
    void setUp()
    {
        connection = connectionMock( BoltProtocolV4.INSTANCE );
        connectionProvider = mock( ConnectionProvider.class );
        when( connectionProvider.acquireConnection( any( String.class ), any( AccessMode.class ) ) )
                .thenReturn( completedFuture( connection ) );
        session = new InternalSession( newSession( connectionProvider ) );
    }

    private static Stream<Function<Session,StatementResult>> allSessionRunMethods()
    {
        return Stream.of(
                session -> session.run( "RETURN 1" ),
                session -> session.run( "RETURN $x", parameters( "x", 1 ) ),
                session -> session.run( "RETURN $x", singletonMap( "x", 1 ) ),
                session -> session.run( "RETURN $x",
                        new InternalRecord( singletonList( "x" ), new Value[]{new IntegerValue( 1 )} ) ),
                session -> session.run( new Statement( "RETURN $x", parameters( "x", 1 ) ) ),
                session -> session.run( new Statement( "RETURN $x", parameters( "x", 1 ) ), empty() ),
                session -> session.run( "RETURN $x", singletonMap( "x", 1 ), empty() ),
                session -> session.run( "RETURN 1", empty() )
        );
    }

    private static Stream<Function<Session,Transaction>> allBeginTxMethods()
    {
        return Stream.of(
                session -> session.beginTransaction(),
                session -> session.beginTransaction( TransactionConfig.empty() )
        );
    }

    private static Stream<Function<Session,String>> allRunTxMethods()
    {
        return Stream.of(
                session -> session.readTransaction( tx -> "a" ),
                session -> session.writeTransaction( tx -> "a" ),
                session -> session.readTransaction( tx -> "a", empty() ),
                session -> session.writeTransaction( tx -> "a", empty() )
        );
    }

    @ParameterizedTest
    @MethodSource( "allSessionRunMethods" )
    void shouldFlushOnRun( Function<Session,StatementResult> runReturnOne ) throws Throwable
    {
        setupSuccessfulRunAndPull( connection );

        StatementResult result = runReturnOne.apply( session );
        ResultSummary summary = result.summary();

        verifyRunAndPull( connection, summary.statement().text() );
    }

    @ParameterizedTest
    @MethodSource( "allBeginTxMethods" )
    void shouldBeginTx( Function<Session,Transaction> beginTx ) throws Throwable
    {
        Transaction tx = beginTx.apply( session );

        verifyBeginTx( connection );
        assertNotNull( tx );
    }

    @ParameterizedTest
    @MethodSource( "allRunTxMethods" )
    void runTxShouldBeginTxAndCloseTx( Function<Session,String> runTx ) throws Throwable
    {
        String string = runTx.apply( session );

        verifyBeginTx( connection );
        verify( connection ).writeAndFlush( any( CommitMessage.class ), any() );
        verify( connection ).release();
        assertThat( string, equalTo( "a" ) );
    }

    @Test
    void shouldNotAllowNewTxWhileOneIsRunning()
    {
        // Given
        session.beginTransaction();

        // Expect
        assertThrows( ClientException.class, session::beginTransaction );
    }

    @Test
    void shouldBeAbleToOpenTxAfterPreviousIsClosed()
    {
        // Given
        session.beginTransaction().close();

        // When
        Transaction tx = session.beginTransaction();

        // Then we should've gotten a transaction object back
        assertNotNull( tx );
    }

    @Test
    void shouldNotBeAbleToUseSessionWhileOngoingTransaction()
    {
        // Given
        session.beginTransaction();

        // Expect
        assertThrows( ClientException.class, () -> session.run( "RETURN 1" ) );
    }

    @Test
    void shouldBeAbleToUseSessionAgainWhenTransactionIsClosed()
    {
        // Given
        session.beginTransaction().close();

        // When
        session.run( "RETURN 1" );

        // Then
        verifyRunAndPull( connection, "RETURN 1" );
    }

    @Test
    void shouldNotCloseAlreadyClosedSession()
    {
        Transaction tx = session.beginTransaction();

        session.close();
        session.close();
        session.close();

        verifyRollbackTx( connection );
    }

    @Test
    void runThrowsWhenSessionIsClosed()
    {
        session.close();

        Exception e = assertThrows( Exception.class, () -> session.run( "CREATE ()" ) );
        assertThat( e, instanceOf( ClientException.class ) );
        assertThat( e.getMessage(), containsString( "session is already closed" ) );
    }

    @Test
    void acquiresNewConnectionForRun()
    {
        session.run( "RETURN 1" );

        verify( connectionProvider ).acquireConnection( any( String.class ), any( AccessMode.class ) );
    }

    @Test
    void releasesOpenConnectionUsedForRunWhenSessionIsClosed()
    {
        String query = "RETURN 1";
        setupSuccessfulRunAndPull( connection, query );

        session.run( query );
        session.close();

        InOrder inOrder = inOrder( connection );
        inOrder.verify( connection ).writeAndFlush( any( RunWithMetadataMessage.class ), any(), any( PullMessage.class ), any() );
        inOrder.verify( connection, atLeastOnce() ).release();
    }

    @SuppressWarnings( "deprecation" )
    @Test
    void resetDoesNothingWhenNoTransactionAndNoConnection()
    {
        session.reset();

        verify( connectionProvider, never() ).acquireConnection( any( String.class ), any( AccessMode.class ) );
    }

    @Test
    void closeWithoutConnection()
    {
        session.close();

        verify( connectionProvider, never() ).acquireConnection( any( String.class ), any( AccessMode.class ) );
    }

    @Test
    void acquiresNewConnectionForBeginTx()
    {
        Transaction tx = session.beginTransaction();

        assertNotNull( tx );
        verify( connectionProvider ).acquireConnection( any( String.class ), any( AccessMode.class ) );
    }

    @Test
    void updatesBookmarkWhenTxIsClosed()
    {
        Bookmarks bookmarkAfterCommit = Bookmarks.from( "TheBookmark" );

        BoltProtocol protocol = spy( BoltProtocolV4.INSTANCE );
        doReturn( completedFuture( bookmarkAfterCommit ) ).when( protocol ).commitTransaction( any( Connection.class ) );

        when( connection.protocol() ).thenReturn( protocol );

        Transaction tx = session.beginTransaction();
        assertNull( session.lastBookmark() );

        tx.success();
        tx.close();
        assertEquals( "TheBookmark", session.lastBookmark() );
    }

    @Test
    void releasesConnectionWhenTxIsClosed()
    {
        String query = "RETURN 42";
        setupSuccessfulRunAndPull( connection, query );

        Transaction tx = session.beginTransaction();
        tx.run( query );

        verify( connectionProvider ).acquireConnection( any( String.class ), any( AccessMode.class ) );
        verifyRunAndPull( connection, query );

        tx.close();
        verify( connection ).release();
    }

    @Test
    void bookmarkIsPropagatedFromSession()
    {
        Bookmarks bookmarks = Bookmarks.from( "Bookmarks" );
        Session session = new InternalSession( newSession( connectionProvider, bookmarks ) );

        Transaction tx = session.beginTransaction();
        assertNotNull( tx );
        verifyBeginTx( connection, bookmarks );
    }

    @Test
    void bookmarkIsPropagatedBetweenTransactions()
    {
        Bookmarks bookmarks1 = Bookmarks.from( "Bookmark1" );
        Bookmarks bookmarks2 = Bookmarks.from( "Bookmark2" );

        Session session = new InternalSession( newSession( connectionProvider) );

        BoltProtocol protocol = spy( BoltProtocolV4.INSTANCE );
        doReturn( completedFuture( bookmarks1 ), completedFuture( bookmarks2 ) ).when( protocol ).commitTransaction( any( Connection.class ) );

        when( connection.protocol() ).thenReturn( protocol );

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.success();
        }
        assertEquals( bookmarks1, Bookmarks.from( session.lastBookmark() ) );

        try ( Transaction tx = session.beginTransaction() )
        {
            verifyBeginTx( connection, bookmarks1 );
            tx.success();
        }
        assertEquals( bookmarks2, Bookmarks.from( session.lastBookmark() ) );
    }

    @Test
    void accessModeUsedToAcquireConnections()
    {
        Session session1 = new InternalSession( newSession( connectionProvider, READ ) );
        session1.beginTransaction();
        verify( connectionProvider ).acquireConnection( ABSENT_DB_NAME, READ );

        Session session2 = new InternalSession( newSession( connectionProvider, WRITE ) );
        session2.beginTransaction();
        verify( connectionProvider ).acquireConnection( ABSENT_DB_NAME, WRITE );
    }

    @Test
    void testPassingNoBookmarkShouldRetainBookmark()
    {
        Session session = new InternalSession( newSession( connectionProvider, Bookmarks.from( "X" ) ) );
        session.beginTransaction();
        assertThat( session.lastBookmark(), equalTo( "X" ) );
    }
    
    @Test
    void acquiresReadConnectionForReadTxInReadSession()
    {
        testConnectionAcquisition( READ, READ );
    }

    @Test
    void acquiresWriteConnectionForWriteTxInReadSession()
    {
        testConnectionAcquisition( READ, WRITE );
    }

    @Test
    void acquiresReadConnectionForReadTxInWriteSession()
    {
        testConnectionAcquisition( WRITE, READ );
    }

    @Test
    void acquiresWriteConnectionForWriteTxInWriteSession()
    {
        testConnectionAcquisition( WRITE, WRITE );
    }

    @Test
    void commitsReadTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, true );
    }

    @Test
    void commitsWriteTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( WRITE, true );
    }

    @Test
    void rollsBackReadTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, false );
    }

    @Test
    void rollsBackWriteTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, true );
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
    void connectionShouldBeResetAfterSessionReset()
    {
        session.run( "RETURN 1" );

        verify( connection, never() ).reset();
        verify( connection, never() ).release();

        session.reset();
        verify( connection ).reset();
        verify( connection, never() ).release();
    }

    @Test
    void shouldHaveNullLastBookmarkInitially()
    {
        assertNull( session.lastBookmark() );
    }

    @Test
    void shouldDoNothingWhenClosingWithoutAcquiredConnection()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        when( connectionProvider.acquireConnection( any( String.class ), any( AccessMode.class ) ) ).thenReturn( failedFuture( error ) );

        Exception e = assertThrows( Exception.class, () -> session.run( "RETURN 1" ) );
        assertEquals( error, e );

        session.close();
    }

    @Test
    void shouldRunAfterRunFailureToAcquireConnection()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        when( connectionProvider.acquireConnection( any( String.class ), any( AccessMode.class ) ) )
                .thenReturn( failedFuture( error ) ).thenReturn( completedFuture( connection ) );

        Exception e = assertThrows( Exception.class, () -> session.run( "RETURN 1" ) );
        assertEquals( error, e );

        session.run( "RETURN 2" );

        verify( connectionProvider, times( 2 ) ).acquireConnection( any( String.class ), any( AccessMode.class ) );
        verifyRunAndPull( connection, "RETURN 2" );
    }

    @Test
    void shouldRunAfterBeginTxFailureOnBookmark()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        Connection connection1 = connectionMock( BoltProtocolV4.INSTANCE );
        setupFailingBegin( connection1, error );
        Connection connection2 = connectionMock( BoltProtocolV4.INSTANCE );

        when( connectionProvider.acquireConnection( any( String.class ), any( AccessMode.class ) ) )
                .thenReturn( completedFuture( connection1 ) ).thenReturn( completedFuture( connection2 ) );

        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx42" );
        Session session = new InternalSession( newSession( connectionProvider, bookmarks ) );

        Exception e = assertThrows( Exception.class, session::beginTransaction );
        assertEquals( error, e );

        session.run( "RETURN 2" );

        verify( connectionProvider, times( 2 ) ).acquireConnection( any( String.class ), any( AccessMode.class ) );
        verifyBeginTx( connection1, bookmarks );
        verifyRunAndPull( connection2, "RETURN 2" );
    }

    @Test
    void shouldBeginTxAfterBeginTxFailureOnBookmark()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        Connection connection1 = connectionMock( BoltProtocolV4.INSTANCE );
        setupFailingBegin( connection1, error );
        Connection connection2 = connectionMock( BoltProtocolV4.INSTANCE );

        when( connectionProvider.acquireConnection( any( String.class ), any( AccessMode.class ) ) )
                .thenReturn( completedFuture( connection1 ) ).thenReturn( completedFuture( connection2 ) );

        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx42" );
        Session session = new InternalSession( newSession( connectionProvider, bookmarks ) );

        Exception e = assertThrows( Exception.class, session::beginTransaction );
        assertEquals( error, e );

        session.beginTransaction();

        verify( connectionProvider, times( 2 ) ).acquireConnection( any( String.class ), any( AccessMode.class ) );
        verifyBeginTx( connection1, bookmarks );
        verifyBeginTx( connection2, bookmarks );
    }

    @Test
    void shouldBeginTxAfterRunFailureToAcquireConnection()
    {
        RuntimeException error = new RuntimeException( "Hi" );
        when( connectionProvider.acquireConnection( any( String.class ), any( AccessMode.class ) ) )
                .thenReturn( failedFuture( error ) ).thenReturn( completedFuture( connection ) );

        Exception e = assertThrows( Exception.class, () -> session.run( "RETURN 1" ) );
        assertEquals( error, e );

        session.beginTransaction();

        verify( connectionProvider, times( 2 ) ).acquireConnection( any( String.class ), any( AccessMode.class ) );
        verifyBeginTx( connection );
    }

    @Test
    void shouldMarkTransactionAsTerminatedAndThenResetConnectionOnReset()
    {
        Transaction tx = session.beginTransaction();

        assertTrue( tx.isOpen() );
        verify( connection, never() ).reset();

        session.reset();

        verify( connection ).reset();
    }

    private void testConnectionAcquisition( AccessMode sessionMode, AccessMode transactionMode )
    {
        Session session = new InternalSession( newSession( connectionProvider, sessionMode ) );

        TxWork work = new TxWork( 42 );

        int result = executeTransaction( session, transactionMode, work );

        verify( connectionProvider ).acquireConnection( any( String.class ), eq( transactionMode ) );
        verifyBeginTx( connection );
        verifyCommitTx( connection );
        assertEquals( 42, result );
    }

    private void testTxCommitOrRollback( AccessMode transactionMode, final boolean commit )
    {
        Session session = new InternalSession( newSession( connectionProvider, WRITE ) );

        TransactionWork<Integer> work = tx ->
        {
            if ( !commit )
            {
                tx.failure();
            }
            return 4242;
        };

        int result = executeTransaction( session, transactionMode, work );

        verify( connectionProvider ).acquireConnection( any( String.class ), eq( transactionMode ) );
        verifyBeginTx( connection );
        if ( commit )
        {
            verifyCommitTx( connection );
            verifyRollbackTx( connection, never() );
        }
        else
        {
            verifyRollbackTx( connection );
            verifyCommitTx( connection, never() );
        }
        assertEquals( 4242, result );
    }

    private void testTxRollbackWhenThrows( AccessMode transactionMode )
    {
        final RuntimeException error = new IllegalStateException( "Oh!" );
        TransactionWork<Void> work = tx ->
        {
            throw error;
        };

        Exception e = assertThrows( Exception.class, () -> executeTransaction( session, transactionMode, work ) );
        assertEquals( error, e );

        verify( connectionProvider ).acquireConnection( any( String.class ), eq( transactionMode ) );
        verifyBeginTx( connection );
        verifyRollbackTx( connection );
    }

    private void testTxIsRetriedUntilSuccessWhenFunctionThrows( AccessMode mode )
    {
        int failures = 12;
        int retries = failures + 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        Session session = new InternalSession( newSession( connectionProvider, retryLogic ) );

        TxWork work = spy( new TxWork( 42, failures, new SessionExpiredException( "" ) ) );
        int answer = executeTransaction( session, mode, work );

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
        Session session = new InternalSession( newSession( connectionProvider, retryLogic ) );

        TxWork work = spy( new TxWork( 43 ) );
        int answer = executeTransaction( session, mode, work );

        assertEquals( 43, answer );
        verifyInvocationCount( work, failures + 1 );
        verifyCommitTx( connection, times( retries ) );
    }

    private void testTxIsRetriedUntilFailureWhenFunctionThrows( AccessMode mode )
    {
        int failures = 14;
        int retries = failures - 1;

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        Session session = new InternalSession( newSession( connectionProvider, retryLogic ) );

        TxWork work = spy( new TxWork( 42, failures, new SessionExpiredException( "Oh!" ) ) );

        Exception e = assertThrows( Exception.class, () -> executeTransaction( session, mode, work ) );

        assertThat( e, instanceOf( SessionExpiredException.class ) );
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
        Session session = new InternalSession( newSession( connectionProvider, retryLogic ) );

        TxWork work = spy( new TxWork( 42 ) );

        Exception e = assertThrows( Exception.class, () -> executeTransaction( session, mode, work ) );

        assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        verifyInvocationCount( work, failures );
        verifyCommitTx( connection, times( failures ) );
    }

    private static <T> T executeTransaction( Session session, AccessMode mode, TransactionWork<T> work )
    {
        if ( mode == READ )
        {
            return session.readTransaction( work );
        }
        else if ( mode == WRITE )
        {
            return session.writeTransaction( work );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown mode " + mode );
        }
    }

    private static void verifyInvocationCount( TransactionWork<?> workSpy, int expectedInvocationCount )
    {
        verify( workSpy, times( expectedInvocationCount ) ).execute( any( Transaction.class ) );
    }

    private static class TxWork implements TransactionWork<Integer>
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
        public Integer execute( Transaction tx )
        {
            if ( timesToThrow > 0 && invoked++ < timesToThrow )
            {
                throw errorSupplier.get();
            }
            tx.success();
            return result;
        }
    }
}
