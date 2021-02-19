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

import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactory;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;
import org.neo4j.driver.util.TestUtil;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.retry.RetrySettings.DEFAULT;

@ParallelizableIT
class TransactionIT
{
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldAllowRunRollbackAndClose()
    {
        shouldRunAndCloseAfterAction( Transaction::rollback, false );
    }

    @Test
    void shouldAllowRunCommitAndClose()
    {
        shouldRunAndCloseAfterAction( Transaction::commit, true );
    }

    @Test
    void shouldAllowRunCloseAndClose()
    {
        shouldRunAndCloseAfterAction( Transaction::close, false );
    }

    @Test
    void shouldRunAndRollbackByDefault()
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (n:FirstNode)" );
            tx.run( "CREATE (n:SecondNode)" );
        }

        // Then there should be no visible effect of the transaction
        Result cursor = session.run( "MATCH (n) RETURN count(n)" );
        long nodes = cursor.single().get( "count(n)" ).asLong();
        assertThat( nodes, equalTo( 0L ) );
    }

    @Test
    void shouldRetrieveResults()
    {
        // Given
        session.run( "CREATE (n {name:'Steve Brook'})" );

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            Result res = tx.run( "MATCH (n) RETURN n.name" );

            // Then
            assertThat( res.single().get( "n.name" ).asString(), equalTo( "Steve Brook" ) );
        }
    }

    @Test
    void shouldNotAllowSessionLevelQueriesWhenThereIsATransaction()
    {
        session.beginTransaction();

        assertThrows( ClientException.class, () -> session.run( "anything" ) );
    }

    @Test
    void shouldFailToRunQueryAfterTxIsCommitted()
    {
        shouldFailToRunQueryAfterTxAction( Transaction::commit );
    }

    @Test
    void shouldFailToRunQueryAfterTxIsRolledBack()
    {
        shouldFailToRunQueryAfterTxAction( Transaction::rollback );
    }

    @Test
    void shouldFailToRunQueryAfterTxIsClosed()
    {
        shouldFailToRunQueryAfterTxAction( Transaction::close );
    }

    @Test
    void shouldFailToCommitAfterRolledBack()
    {
        Transaction tx = session.beginTransaction();
        tx.run( "CREATE (:MyLabel)" );
        tx.rollback();

        ClientException e = assertThrows( ClientException.class, tx::commit );
        assertThat( e.getMessage(), startsWith( "Can't commit, transaction has been rolled back" ) );
    }

    @Test
    void shouldFailToRollbackAfterTxIsCommitted()
    {
        Transaction tx = session.beginTransaction();
        tx.run( "CREATE (:MyLabel)" );
        tx.commit();

        ClientException e = assertThrows( ClientException.class, tx::rollback );
        assertThat( e.getMessage(), startsWith( "Can't rollback, transaction has been committed" ) );
    }

    @Test
    void shouldFailToCommitAfterCommit() throws Throwable
    {
        Transaction tx = session.beginTransaction();
        tx.run( "CREATE (:MyLabel)" );
        tx.commit();

        ClientException e = assertThrows( ClientException.class, tx::commit );
        assertThat( e.getMessage(), startsWith( "Can't commit, transaction has been committed" ) );
    }

    @Test
    void shouldFailToRollbackAfterRollback() throws Throwable
    {
        Transaction tx = session.beginTransaction();
        tx.run( "CREATE (:MyLabel)" );
        tx.rollback();

        ClientException e = assertThrows( ClientException.class, tx::rollback );
        assertThat( e.getMessage(), startsWith( "Can't rollback, transaction has been rolled back" ) );
    }


    @Test
    void shouldBeClosedAfterClose()
    {
        shouldBeClosedAfterAction( Transaction::close );
    }

    @Test
    void shouldBeClosedAfterRollback()
    {
        shouldBeClosedAfterAction( Transaction::rollback );
    }

    @Test
    void shouldBeClosedAfterCommit()
    {
        shouldBeClosedAfterAction( Transaction::commit );
    }

    @Test
    void shouldBeOpenBeforeCommit()
    {
        // When
        Transaction tx = session.beginTransaction();

        // Then
        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldHandleNullParametersGracefully()
    {
        // When
        session.run( "match (n) return count(n)", (Value) null );

        // Then
        // pass - no exception thrown

    }

    //See GH #146
    @Test
    void shouldHandleFailureAfterClosingTransaction()
    {
        // GIVEN a successful query in a transaction
        Transaction tx = session.beginTransaction();
        Result result = tx.run( "CREATE (n) RETURN n" );
        result.consume();
        tx.commit();
        tx.close();

        // WHEN when running a malformed query in the original session
        assertThrows( ClientException.class, () -> session.run( "CREAT (n) RETURN n" ).consume() );
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    void shouldHandleNullRecordParameters()
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            Record params = null;
            tx.run( "CREATE (n:FirstNode)", params );
            tx.commit();
        }

        // Then it wasn't the end of the world as we know it
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    void shouldHandleNullValueParameters()
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            Value params = null;
            tx.run( "CREATE (n:FirstNode)", params );
            tx.commit();
        }

        // Then it wasn't the end of the world as we know it
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    void shouldHandleNullMapParameters()
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            Map<String,Object> params = null;
            tx.run( "CREATE (n:FirstNode)", params );
            tx.commit();
        }

        // Then it wasn't the end of the world as we know it
    }

    @Test
    void shouldRollBackTxIfErrorWithoutConsume()
    {
        // Given
        Transaction tx = session.beginTransaction();
        tx.run( "invalid" ); // send run, pull_all

        assertThrows( ClientException.class, tx::commit );

        try ( Transaction anotherTx = session.beginTransaction() )
        {
            Result cursor = anotherTx.run( "RETURN 1" );
            int val = cursor.single().get( "1" ).asInt();
            assertThat( val, equalTo( 1 ) );
        }
    }

    @Test
    void shouldRollBackTxIfErrorWithConsume()
    {
        assertThrows( ClientException.class, () ->
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                Result result = tx.run( "invalid" );
                result.consume();
            }
        } );

        try ( Transaction tx = session.beginTransaction() )
        {
            Result cursor = tx.run( "RETURN 1" );
            int val = cursor.single().get( "1" ).asInt();
            assertThat( val, equalTo( 1 ) );
        }
    }

    @Test
    void shouldPropagateFailureFromSummary()
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            Result result = tx.run( "RETURN Wrong" );

            ClientException e = assertThrows( ClientException.class, result::consume );
            assertThat( e.code(), containsString( "SyntaxError" ) );
            assertNotNull( result.consume() );
        }
    }

    @Test
    void shouldBeResponsiveToThreadInterruptWhenWaitingForResult()
    {
        try ( Session otherSession = session.driver().session() )
        {
            session.run( "CREATE (:Person {name: 'Beta Ray Bill'})" ).consume();

            Transaction tx1 = session.beginTransaction();
            Transaction tx2 = otherSession.beginTransaction();
            tx1.run( "MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Mjolnir'" ).consume();

            // now 'Beta Ray Bill' node is locked

            // setup other thread to interrupt current thread when it blocks
            TestUtil.interruptWhenInWaitingState( Thread.currentThread() );

            try
            {
                ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class,
                        () -> tx2.run( "MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Stormbreaker'" ).consume() );
                assertThat( e.getMessage(), containsString( "Connection to the database terminated" ) );
                assertThat( e.getMessage(), containsString( "Thread interrupted while waiting for result to arrive" ) );
            }
            finally
            {
                // clear interrupted flag
                Thread.interrupted();
            }
        }
    }

    @Test
    void shouldBeResponsiveToThreadInterruptWhenWaitingForCommit()
    {
        try ( Session otherSession = session.driver().session() )
        {
            session.run( "CREATE (:Person {name: 'Beta Ray Bill'})" ).consume();

            Transaction tx1 = session.beginTransaction();
            Transaction tx2 = otherSession.beginTransaction();
            tx1.run( "MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Mjolnir'" ).consume();

            // now 'Beta Ray Bill' node is locked

            tx2.run( "MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Stormbreaker'" );

            // setup other thread to interrupt current thread when it blocks
            TestUtil.interruptWhenInWaitingState( Thread.currentThread() );

            try
            {
                assertThrows( ServiceUnavailableException.class, tx2::commit );
            }
            finally
            {
                // clear interrupted flag
                Thread.interrupted();
            }
        }
    }

    @Test
    void shouldThrowWhenConnectionKilledDuringTransaction()
    {
        ChannelTrackingDriverFactory factory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );
        Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).build();

        try ( Driver driver = factory.newInstance( session.uri(), session.authToken(), RoutingSettings.DEFAULT, DEFAULT, config, SecurityPlanImpl.insecure() ) )
        {
            ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () ->
            {
                try ( Session session1 = driver.session();
                      Transaction tx = session1.beginTransaction() )
                {
                    tx.run( "CREATE (:MyNode {id: 1})" ).consume();

                    // kill all network channels
                    for ( Channel channel: factory.channels() )
                    {
                        channel.close().syncUninterruptibly();
                    }

                    tx.run( "CREATE (:MyNode {id: 1})" ).consume();
                }
            } );

            assertThat( e.getMessage(), containsString( "Connection to the database terminated" ) );
        }

        assertEquals( 0, session.run( "MATCH (n:MyNode {id: 1}) RETURN count(n)" ).single().get( 0 ).asInt() );
    }

    @Test
    void shouldFailToCommitAfterFailure() throws Throwable
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            List<Integer> xs = tx.run( "UNWIND [1,2,3] AS x CREATE (:Node) RETURN x" ).list( record -> record.get( 0 ).asInt() );
            assertEquals( asList( 1, 2, 3 ), xs );

            ClientException error1 = assertThrows( ClientException.class, () -> tx.run( "RETURN unknown" ).consume() );
            assertThat( error1.code(), containsString( "SyntaxError" ) );

            ClientException error2 = assertThrows( ClientException.class, tx::commit );
            assertThat( error2.getMessage(), startsWith( "Transaction can't be committed. It has been rolled back" ) );
        }
    }

    @Test
    void shouldDisallowQueriesAfterFailureWhenResultsAreConsumed()
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            List<Integer> xs = tx.run( "UNWIND [1,2,3] AS x CREATE (:Node) RETURN x" ).list( record -> record.get( 0 ).asInt() );
            assertEquals( asList( 1, 2, 3 ), xs );

            ClientException error1 = assertThrows( ClientException.class, () -> tx.run( "RETURN unknown" ).consume() );
            assertThat( error1.code(), containsString( "SyntaxError" ) );

            ClientException error2 = assertThrows( ClientException.class, () -> tx.run( "CREATE (:OtherNode)" ).consume() );
            assertThat( error2.getMessage(), startsWith( "Cannot run more queries in this transaction" ) );

            ClientException error3 = assertThrows( ClientException.class, () -> tx.run( "RETURN 42" ).consume() );
            assertThat( error3.getMessage(), startsWith( "Cannot run more queries in this transaction" ) );
        }

        assertEquals( 0, countNodesByLabel( "Node" ) );
        assertEquals( 0, countNodesByLabel( "OtherNode" ) );
    }

    @Test
    void shouldRollbackWhenMarkedSuccessfulButOneQueryFails()
    {
        ClientException error = assertThrows( ClientException.class, () ->
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (:Node1)" );
                tx.run( "CREATE (:Node2)" );
                tx.run( "CREATE SmthStrange" );
                tx.run( "CREATE (:Node3)" );
                tx.run( "CREATE (:Node4)" );

                tx.commit();
            }
        } );

        assertThat( error.code(), containsString( "SyntaxError" ) );
        assertThat( error.getSuppressed().length, greaterThanOrEqualTo( 1 ) );
        Throwable suppressed = error.getSuppressed()[0];
        assertThat( suppressed, instanceOf( ClientException.class ) );
        assertThat( suppressed.getMessage(), startsWith( "Transaction can't be committed" ) );

        assertEquals( 0, countNodesByLabel( "Node1" ) );
        assertEquals( 0, countNodesByLabel( "Node2" ) );
        assertEquals( 0, countNodesByLabel( "Node3" ) );
        assertEquals( 0, countNodesByLabel( "Node4" ) );
    }

    private void shouldRunAndCloseAfterAction( Consumer<Transaction> txConsumer, boolean isCommit )
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (n:FirstNode)" );
            tx.run( "CREATE (n:SecondNode)" );
            txConsumer.accept( tx );
        }

        // Then the outcome of both queries should be visible
        Result result = session.run( "MATCH (n) RETURN count(n)" );
        long nodes = result.single().get( "count(n)" ).asLong();
        if (isCommit)
        {
            assertThat( nodes, equalTo( 2L ) );
        }
        else
        {
            assertThat( nodes, equalTo( 0L ) );
        }
    }

    private void shouldBeClosedAfterAction( Consumer<Transaction> txConsumer )
    {
        // When
        Transaction tx = session.beginTransaction();
        txConsumer.accept( tx );

        // Then
        assertFalse( tx.isOpen() );
    }

    private void shouldFailToRunQueryAfterTxAction( Consumer<Transaction> txConsumer )
    {
        Transaction tx = session.beginTransaction();
        tx.run( "CREATE (:MyLabel)" );
        txConsumer.accept( tx );

        ClientException e = assertThrows( ClientException.class, () -> tx.run( "CREATE (:MyOtherLabel)" ) );
        assertThat( e.getMessage(), startsWith( "Cannot run more queries in this transaction" ) );
    }

    private static int countNodesByLabel( String label )
    {
        return session.run( "MATCH (n:" + label + ") RETURN count(n)" ).single().get( 0 ).asInt();
    }
}
