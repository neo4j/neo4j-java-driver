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
package org.neo4j.driver.integration;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.QueryRunner;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.util.DaemonThreadFactory.daemon;
import static org.neo4j.driver.util.Neo4jRunner.HOME_DIR;
import static org.neo4j.driver.util.Neo4jSettings.IMPORT_DIR;
import static org.neo4j.driver.util.Neo4jSettings.TEST_SETTINGS;
import static org.neo4j.driver.util.TestUtil.activeQueryCount;
import static org.neo4j.driver.util.TestUtil.activeQueryNames;
import static org.neo4j.driver.util.TestUtil.awaitAllFutures;
import static org.neo4j.driver.util.TestUtil.awaitCondition;

@SuppressWarnings( "deprecation" )
@ParallelizableIT
class SessionResetIT
{
    private static final int CSV_FILE_SIZE = 10_000;
    private static final int LOAD_CSV_BATCH_SIZE = 10;

    private static final String SHORT_QUERY_1 = "CREATE (n:Node {name: 'foo', occupation: 'bar'})";
    private static final String SHORT_QUERY_2 = "MATCH (n:Node {name: 'foo'}) RETURN count(n)";
    private static final String LONG_QUERY = "UNWIND range(0, 10000000) AS i CREATE (n:Node {idx: i}) DELETE n";
    private static final String LONG_PERIODIC_COMMIT_QUERY_TEMPLATE =
            "USING PERIODIC COMMIT 1 " +
            "LOAD CSV FROM '%s' AS line " +
            "UNWIND range(1, " + LOAD_CSV_BATCH_SIZE + ") AS index " +
            "CREATE (n:Node {id: index, name: line[0], occupation: line[1]})";

    private static final int STRESS_TEST_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private static final long STRESS_TEST_DURATION_MS = SECONDS.toMillis( 5 );
    private static final String[] STRESS_TEST_QUERIES = {SHORT_QUERY_1, SHORT_QUERY_2, LONG_QUERY};

    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private ExecutorService executor;

    @BeforeEach
    void setUp()
    {
        executor = Executors.newCachedThreadPool( daemon( getClass().getSimpleName() + "-thread" ) );
    }

    @AfterEach
    void tearDown()
    {
        if ( executor != null )
        {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldTerminateAutoCommitQuery()
    {
        testQueryTermination( LONG_QUERY, true );
    }

    @Test
    void shouldTerminateQueryInUnmanagedTransaction()
    {
        testQueryTermination( LONG_QUERY, false );
    }

    /**
     * It is currently unsafe to terminate periodic commit query because it'll then be half-committed.
     * So the driver give no guarantee when the periodic commit could be terminated.
     * For a user who want to terminate a periodic commit, he or she should use kill query by id.
     */
    @Test
    void shouldTerminatePeriodicCommitQueryRandomly()
    {
        Future<Void> queryResult = runQueryInDifferentThreadAndResetSession( longPeriodicCommitQuery(), true );

        ExecutionException e = assertThrows( ExecutionException.class, () -> queryResult.get( 1, MINUTES ) );
        assertThat( e.getCause(), instanceOf( Neo4jException.class ) );

        awaitNoActiveQueries();

        assertThat( countNodes(), lessThanOrEqualTo( ((long) CSV_FILE_SIZE) * LOAD_CSV_BATCH_SIZE ) );
    }

    @Test
    void shouldTerminateAutoCommitQueriesRandomly() throws Exception
    {
        testRandomQueryTermination( true );
    }

    @Test
    void shouldTerminateQueriesInUnmanagedTransactionsRandomly() throws Exception
    {
        testRandomQueryTermination( false );
    }

    @Test
    void shouldNotAllowBeginTxIfResetFailureIsNotConsumed() throws Throwable
    {
        neo4j.ensureProcedures( "longRunningStatement.jar" );

        try ( Session session = neo4j.driver().session() )
        {
            Transaction tx1 = session.beginTransaction();

            Result result = tx1.run( "CALL test.driver.longRunningStatement({seconds})", parameters( "seconds", 10 ) );

            awaitActiveQueriesToContain( "CALL test.driver.longRunningStatement" );
            session.reset();

            ClientException e1 = assertThrows( ClientException.class, session::beginTransaction );
            assertThat( e1.getMessage(), containsString( "You cannot begin a transaction on a session with an open transaction" ) );

            ClientException e2 = assertThrows( ClientException.class, () -> tx1.run( "RETURN 1" ) );
            assertThat( e2.getMessage(), containsString( "Cannot run more queries in this transaction" ) );

            // Make sure failure from the terminated long running query is propagated
            Neo4jException e3 = assertThrows( Neo4jException.class, result::consume );
            assertThat( e3.getMessage(), containsString( "The transaction has been terminated" ) );
        }
    }

    @Test
    void shouldThrowExceptionOnCloseIfResetFailureIsNotConsumed() throws Throwable
    {
        neo4j.ensureProcedures( "longRunningStatement.jar" );

        Session session = neo4j.driver().session();
        session.run( "CALL test.driver.longRunningStatement({seconds})",
                parameters( "seconds", 10 ) );

        awaitActiveQueriesToContain( "CALL test.driver.longRunningStatement" );
        session.reset();

        Neo4jException e = assertThrows( Neo4jException.class, session::close );
        assertThat( e.getMessage(), containsString( "The transaction has been terminated" ) );
    }

    @Test
    void shouldBeAbleToBeginTxAfterResetFailureIsConsumed() throws Throwable
    {
        neo4j.ensureProcedures( "longRunningStatement.jar" );

        try ( Session session = neo4j.driver().session() )
        {
            Transaction tx1 = session.beginTransaction();

            Result procedureResult = tx1.run( "CALL test.driver.longRunningStatement({seconds})",
                    parameters( "seconds", 10 ) );

            awaitActiveQueriesToContain( "CALL test.driver.longRunningStatement" );
            session.reset();

            Neo4jException e = assertThrows( Neo4jException.class, procedureResult::consume );
            assertThat( e.getMessage(), containsString( "The transaction has been terminated" ) );
            tx1.close();

            try ( Transaction tx2 = session.beginTransaction() )
            {
                tx2.run( "CREATE (n:FirstNode)" );
                tx2.commit();
            }

            Result result = session.run( "MATCH (n) RETURN count(n)" );
            long nodes = result.single().get( "count(n)" ).asLong();
            MatcherAssert.assertThat( nodes, equalTo( 1L ) );
        }
    }

    @Test
    void shouldKillLongRunningQuery() throws Throwable
    {
        neo4j.ensureProcedures( "longRunningStatement.jar" );

        final int executionTimeout = 10; // 10s
        final int killTimeout = 1; // 1s
        final AtomicLong startTime = new AtomicLong( -1 );
        long endTime;

        assertThrows( Neo4jException.class, () ->
        {
            try ( Session session = neo4j.driver().session() )
            {
                Result result = session.run( "CALL test.driver.longRunningStatement({seconds})",
                        parameters( "seconds", executionTimeout ) );

                resetSessionAfterTimeout( session, killTimeout );

                // When
                startTime.set( System.currentTimeMillis() );
                result.consume(); // blocking to run the query
            }
        } );

        endTime = System.currentTimeMillis();
        assertTrue( startTime.get() > 0 );
        assertTrue( endTime - startTime.get() > killTimeout * 1000 ); // get reset by session.reset
        assertTrue( endTime - startTime.get() < executionTimeout * 1000 / 2 ); // finished before execution finished
    }

    @Test
    void shouldKillLongStreamingResult() throws Throwable
    {
        neo4j.ensureProcedures( "longRunningStatement.jar" );
        // Given
        final int executionTimeout = 10; // 10s
        final int killTimeout = 1; // 1s
        final AtomicInteger recordCount = new AtomicInteger();
        final AtomicLong startTime = new AtomicLong( -1 );
        long endTime;

        Neo4jException e = assertThrows( Neo4jException.class, () ->
        {
            try ( Session session = neo4j.driver().session() )
            {
                Result result = session.run( "CALL test.driver.longStreamingResult({seconds})",
                        parameters( "seconds", executionTimeout ) );

                resetSessionAfterTimeout( session, killTimeout );

                // When
                startTime.set( System.currentTimeMillis() );
                while ( result.hasNext() )
                {
                    result.next();
                    recordCount.incrementAndGet();
                }
            }
        } );

        endTime = System.currentTimeMillis();
        assertThat( e.getMessage(), containsString( "The transaction has been terminated" ) );
        assertThat( recordCount.get(), greaterThan( 1 ) );

        assertTrue( startTime.get() > 0 );
        assertTrue( endTime - startTime.get() > killTimeout * 1000 ); // get reset by session.reset
        assertTrue( endTime - startTime.get() < executionTimeout * 1000 / 2 ); // finished before execution finished
    }

    private void resetSessionAfterTimeout( Session session, int timeout )
    {
        executor.submit( () ->
        {
            try
            {
                Thread.sleep( timeout * 1000 ); // let the query execute for timeout seconds
            }
            catch ( InterruptedException ignore )
            {
            }
            finally
            {
                session.reset(); // reset the session after timeout
            }
        } );
    }

    @Test
    void shouldAllowMoreQueriesAfterSessionReset()
    {
        // Given
        try ( Session session = neo4j.driver().session() )
        {

            session.run( "RETURN 1" ).consume();

            // When reset the state of this session
            session.reset();

            // Then can run successfully more queries without any error
            session.run( "RETURN 2" ).consume();
        }
    }

    @Test
    void shouldAllowMoreTxAfterSessionReset()
    {
        // Given
        try ( Session session = neo4j.driver().session() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "RETURN 1" );
                tx.commit();
            }

            // When reset the state of this session
            session.reset();

            // Then can run more Tx
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "RETURN 2" );
                tx.commit();
            }
        }
    }

    @Test
    void shouldMarkTxAsFailedAndDisallowRunAfterSessionReset()
    {
        // Given
        try ( Session session = neo4j.driver().session() )
        {
            Transaction tx = session.beginTransaction();
            // When reset the state of this session
            session.reset();

            // Then
            Exception e = assertThrows( Exception.class, () ->
            {
                tx.run( "RETURN 1" );
                tx.commit();
            } );
            assertThat( e.getMessage(), startsWith( "Cannot run more queries in this transaction" ) );
        }
    }

    @Test
    void shouldAllowMoreTxAfterSessionResetInTx()
    {
        // Given
        try ( Session session = neo4j.driver().session() )
        {
            try ( Transaction ignore = session.beginTransaction() )
            {
                // When reset the state of this session
                session.reset();
            }

            // Then can run more Tx
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "RETURN 2" );
                tx.commit();
            }
        }
    }

    @Test
    void resetShouldStopQueryWaitingForALock() throws Exception
    {
        testResetOfQueryWaitingForLock( new NodeIdUpdater()
        {
            @Override
            void performUpdate( Driver driver, int nodeId, int newNodeId,
                    AtomicReference<Session> usedSessionRef, CountDownLatch latchToWait ) throws Exception
            {
                try ( Session session = driver.session() )
                {
                    usedSessionRef.set( session );
                    latchToWait.await();
                    Result result = updateNodeId( session, nodeId, newNodeId );
                    result.consume();
                }
            }
        } );
    }

    @Test
    void resetShouldStopTransactionWaitingForALock() throws Exception
    {
        testResetOfQueryWaitingForLock( new NodeIdUpdater()
        {
            @Override
            public void performUpdate( Driver driver, int nodeId, int newNodeId,
                    AtomicReference<Session> usedSessionRef, CountDownLatch latchToWait ) throws Exception
            {
                try ( Session session = neo4j.driver().session();
                      Transaction tx = session.beginTransaction() )
                {
                    usedSessionRef.set( session );
                    latchToWait.await();
                    Result result = updateNodeId( tx, nodeId, newNodeId );
                    result.consume();
                }
            }
        } );
    }

    @Test
    void resetShouldStopWriteTransactionWaitingForALock() throws Exception
    {
        AtomicInteger invocationsOfWork = new AtomicInteger();

        testResetOfQueryWaitingForLock( new NodeIdUpdater()
        {
            @Override
            public void performUpdate( Driver driver, int nodeId, int newNodeId,
                    AtomicReference<Session> usedSessionRef, CountDownLatch latchToWait ) throws Exception
            {
                try ( Session session = driver.session() )
                {
                    usedSessionRef.set( session );
                    latchToWait.await();

                    session.writeTransaction( tx ->
                    {
                        invocationsOfWork.incrementAndGet();
                        Result result = updateNodeId( tx, nodeId, newNodeId );
                        result.consume();
                        return null;
                    } );
                }
            }
        } );

        assertEquals( 1, invocationsOfWork.get() );
    }

    @Test
    void shouldBeAbleToRunMoreQueriesAfterResetOnNoErrorState()
    {
        try ( Session session = neo4j.driver().session() )
        {
            // Given
            session.reset();

            // When
            Transaction tx = session.beginTransaction();
            tx.run( "CREATE (n:FirstNode)" );
            tx.commit();

            // Then the outcome of both queries should be visible
            Result result = session.run( "MATCH (n) RETURN count(n)" );
            long nodes = result.single().get( "count(n)" ).asLong();
            assertThat( nodes, equalTo( 1L ) );
        }
    }

    @Test
    void shouldHandleResetBeforeRun()
    {
        try ( Session session = neo4j.driver().session();
              Transaction tx = session.beginTransaction() )
        {
            session.reset();

            ClientException e = assertThrows( ClientException.class, () -> tx.run( "CREATE (n:FirstNode)" ) );
            assertThat( e.getMessage(), containsString( "Cannot run more queries in this transaction" ) );
        }
    }

    @Test
    void shouldHandleResetFromMultipleThreads() throws Throwable
    {
        Session session = neo4j.driver().session();

        CountDownLatch beforeCommit = new CountDownLatch( 1 );
        CountDownLatch afterReset = new CountDownLatch( 1 );

        Future<Void> txFuture = executor.submit( () ->
        {
            Transaction tx1 = session.beginTransaction();
            tx1.run( "CREATE (n:FirstNode)" );
            beforeCommit.countDown();
            afterReset.await();

            // session has been reset, it should not be possible to commit the transaction
            try
            {
                tx1.commit();
            }
            catch ( Neo4jException ignore )
            {
            }

            try ( Transaction tx2 = session.beginTransaction() )
            {
                tx2.run( "CREATE (n:SecondNode)" );
                tx2.commit();
            }

            return null;
        } );

        Future<Void> resetFuture = executor.submit( () ->
        {
            beforeCommit.await();
            session.reset();
            afterReset.countDown();
            return null;
        } );

        executor.shutdown();
        executor.awaitTermination( 20, SECONDS );

        txFuture.get( 20, SECONDS );
        resetFuture.get( 20, SECONDS );

        assertEquals( 0, countNodes( "FirstNode" ) );
        assertEquals( 1, countNodes( "SecondNode" ) );
    }

    private void testResetOfQueryWaitingForLock( NodeIdUpdater nodeIdUpdater ) throws Exception
    {
        int nodeId = 42;
        int newNodeId1 = 4242;
        int newNodeId2 = 424242;

        createNodeWithId( nodeId );

        CountDownLatch nodeLocked = new CountDownLatch( 1 );
        AtomicReference<Session> otherSessionRef = new AtomicReference<>();

        try ( Session session = neo4j.driver().session();
              Transaction tx = session.beginTransaction() )
        {
            Future<Void> txResult = nodeIdUpdater.update( nodeId, newNodeId1, otherSessionRef, nodeLocked );

            Result result = updateNodeId( tx, nodeId, newNodeId2 );
            result.consume();

            nodeLocked.countDown();
            // give separate thread some time to block on a lock
            Thread.sleep( 2_000 );
            otherSessionRef.get().reset();

            assertTransactionTerminated( txResult );
            tx.commit();
        }

        try ( Session session = neo4j.driver().session() )
        {
            Result result = session.run( "MATCH (n) RETURN n.id AS id" );
            int value = result.single().get( "id" ).asInt();
            assertEquals( newNodeId2, value );
        }
    }

    private void createNodeWithId( int id )
    {
        try ( Session session = neo4j.driver().session() )
        {
            session.run( "CREATE (n {id: $id})", parameters( "id", id ) );
        }
    }

    private static Result updateNodeId(QueryRunner queryRunner, int currentId, int newId )
    {
        return queryRunner.run( "MATCH (n {id: $currentId}) SET n.id = $newId",
                parameters( "currentId", currentId, "newId", newId ) );
    }

    private static void assertTransactionTerminated( Future<Void> work )
    {
        ExecutionException e = assertThrows( ExecutionException.class, () -> work.get( 20, TimeUnit.SECONDS ) );
        assertThat( e.getCause(), CoreMatchers.instanceOf( TransientException.class ) );
        assertThat( e.getCause().getMessage(), startsWith( "The transaction has been terminated" ) );
    }

    private void testRandomQueryTermination( boolean autoCommit ) throws Exception
    {
        Set<Session> runningSessions = newSetFromMap( new ConcurrentHashMap<>() );
        AtomicBoolean stop = new AtomicBoolean();
        List<Future<?>> futures = new ArrayList<>();

        for ( int i = 0; i < STRESS_TEST_THREAD_COUNT; i++ )
        {
            futures.add( executor.submit( () ->
            {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while ( !stop.get() )
                {
                    runRandomQuery( autoCommit, random, runningSessions, stop );
                }
            } ) );
        }

        long deadline = System.currentTimeMillis() + STRESS_TEST_DURATION_MS;
        while ( !stop.get() )
        {
            if ( System.currentTimeMillis() > deadline )
            {
                stop.set( true );
            }

            resetAny( runningSessions );

            MILLISECONDS.sleep( 30 );
        }

        awaitAllFutures( futures );
        awaitNoActiveQueries();
    }

    private void runRandomQuery( boolean autoCommit, Random random, Set<Session> runningSessions, AtomicBoolean stop )
    {
        try
        {
            Session session = neo4j.driver().session();
            runningSessions.add( session );
            try
            {
                String query = STRESS_TEST_QUERIES[random.nextInt( STRESS_TEST_QUERIES.length - 1 )];
                runQuery( session, query, autoCommit );
            }
            finally
            {
                runningSessions.remove( session );
                session.close();
            }
        }
        catch ( Throwable error )
        {
            if ( !stop.get() && !isAcceptable( error ) )
            {
                stop.set( true );
                throw error;
            }
            // else it is fine to receive some errors from the driver because
            // sessions are being reset concurrently by the main thread, driver can also be closed concurrently
        }
    }

    private void testQueryTermination( String query, boolean autoCommit )
    {
        Future<Void> queryResult = runQueryInDifferentThreadAndResetSession( query, autoCommit );
        ExecutionException e = assertThrows( ExecutionException.class, () -> queryResult.get( 10, SECONDS ) );
        assertThat( e.getCause(), instanceOf( Neo4jException.class ) );
        awaitNoActiveQueries();
    }

    private Future<Void> runQueryInDifferentThreadAndResetSession( String query, boolean autoCommit )
    {
        AtomicReference<Session> sessionRef = new AtomicReference<>();

        Future<Void> queryResult = runAsync( () ->
        {
            Session session = neo4j.driver().session();
            sessionRef.set( session );
            runQuery( session, query, autoCommit );
        } );

        awaitActiveQueriesToContain( query );

        Session session = sessionRef.get();
        assertNotNull( session );
        session.reset();

        return queryResult;
    }

    private static void runQuery( Session session, String query, boolean autoCommit )
    {
        if ( autoCommit )
        {
            session.run( query ).consume();
        }
        else
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( query );
                tx.commit();
            }
        }
    }

    private void awaitNoActiveQueries()
    {
        awaitCondition( () -> activeQueryCount( neo4j.driver() ) == 0 );
    }

    private void awaitActiveQueriesToContain( String value )
    {
        awaitCondition( () ->
                activeQueryNames( neo4j.driver() ).stream().anyMatch( query -> query.contains( value ) ) );
    }

    private long countNodes()
    {
        return countNodes( null );
    }

    private long countNodes( String label )
    {
        try ( Session session = neo4j.driver().session() )
        {
            Result result = session.run( "MATCH (n" + (label == null ? "" : ":" + label) + ") RETURN count(n) AS result" );
            return result.single().get( 0 ).asLong();
        }
    }

    private static void resetAny( Set<Session> sessions )
    {
        sessions.stream().findAny().ifPresent( session ->
        {
            if ( sessions.remove( session ) )
            {
                resetSafely( session );
            }
        } );
    }

    private static void resetSafely( Session session )
    {
        try
        {
            if ( session.isOpen() )
            {
                session.reset();
            }
        }
        catch ( ClientException e )
        {
            if ( session.isOpen() )
            {
                throw e;
            }
            // else this thread lost race with close and it's fine
        }
    }

    private static boolean isAcceptable( Throwable error )
    {
        // get the root cause
        while ( error.getCause() != null )
        {
            error = error.getCause();
        }

        return isTransactionTerminatedException( error ) ||
               error instanceof ServiceUnavailableException ||
               error instanceof ClientException ||
               error instanceof ClosedChannelException;
    }

    private static boolean isTransactionTerminatedException( Throwable error )
    {
        return error instanceof TransientException &&
               error.getMessage().startsWith( "The transaction has been terminated" ) ||
               error.getMessage().startsWith( "Trying to execute query in a terminated transaction" );
    }

    private static String longPeriodicCommitQuery()
    {
        URI fileUri = createTmpCsvFile();
        return String.format( LONG_PERIODIC_COMMIT_QUERY_TEMPLATE, fileUri );
    }

    private static URI createTmpCsvFile()
    {
        try
        {
            Path importDir = Paths.get( HOME_DIR, TEST_SETTINGS.propertiesMap().get( IMPORT_DIR ) );
            Path csvFile = Files.createTempFile( importDir, "test", ".csv" );
            Iterable<String> lines = range( 0, CSV_FILE_SIZE ).mapToObj( i -> "Foo-" + i + ", Bar-" + i )::iterator;
            return URI.create( "file:///" + Files.write( csvFile, lines ).getFileName() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private abstract class NodeIdUpdater
    {
        final Future<Void> update( int nodeId, int newNodeId, AtomicReference<Session> usedSessionRef,
                CountDownLatch latchToWait )
        {
            return executor.submit( () ->
            {
                performUpdate( neo4j.driver(), nodeId, newNodeId, usedSessionRef, latchToWait );
                return null;
            } );
        }

        abstract void performUpdate( Driver driver, int nodeId, int newNodeId,
                AtomicReference<Session> usedSessionRef, CountDownLatch latchToWait ) throws Exception;
    }
}
