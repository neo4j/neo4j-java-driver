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
package org.neo4j.driver.integration;

import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactory;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;
import org.neo4j.driver.util.TestUtil;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
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
    void shouldRunAndCommit()
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (n:FirstNode)" );
            tx.run( "CREATE (n:SecondNode)" );
            tx.success();
        }

        // Then the outcome of both statements should be visible
        StatementResult result = session.run( "MATCH (n) RETURN count(n)" );
        long nodes = result.single().get( "count(n)" ).asLong();
        assertThat( nodes, equalTo( 2L ) );
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
        StatementResult cursor = session.run( "MATCH (n) RETURN count(n)" );
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
            StatementResult res = tx.run( "MATCH (n) RETURN n.name" );

            // Then
            assertThat( res.single().get( "n.name" ).asString(), equalTo( "Steve Brook" ) );
        }
    }

    @Test
    void shouldNotAllowSessionLevelStatementsWhenThereIsATransaction()
    {
        session.beginTransaction();

        assertThrows( ClientException.class, () -> session.run( "anything" ) );
    }

    @Test
    void shouldBeClosedAfterRollback()
    {
        // When
        Transaction tx = session.beginTransaction();
        tx.close();

        // Then
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldBeClosedAfterCommit()
    {
        // When
        Transaction tx = session.beginTransaction();
        tx.success();
        tx.close();

        // Then
        assertFalse( tx.isOpen() );
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
        StatementResult result = tx.run( "CREATE (n) RETURN n" );
        result.consume();
        tx.success();
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
            tx.success();
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
            tx.success();
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
            tx.success();
        }

        // Then it wasn't the end of the world as we know it
    }

    @Test
    void shouldRollBackTxIfErrorWithoutConsume()
    {
        // Given
        Transaction tx = session.beginTransaction();
        tx.run( "invalid" ); // send run, pull_all
        tx.success();

        assertThrows( ClientException.class, tx::close );

        try ( Transaction anotherTx = session.beginTransaction() )
        {
            StatementResult cursor = anotherTx.run( "RETURN 1" );
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
                StatementResult result = tx.run( "invalid" );
                tx.success();
                result.consume();
            }
        } );

        try ( Transaction tx = session.beginTransaction() )
        {
            StatementResult cursor = tx.run( "RETURN 1" );
            int val = cursor.single().get( "1" ).asInt();
            assertThat( val, equalTo( 1 ) );
        }
    }

    @Test
    void shouldPropagateFailureFromSummary()
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            StatementResult result = tx.run( "RETURN Wrong" );

            ClientException e = assertThrows( ClientException.class, result::summary );
            assertThat( e.code(), containsString( "SyntaxError" ) );
            assertNotNull( result.summary() );
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
            tx2.success();

            // setup other thread to interrupt current thread when it blocks
            TestUtil.interruptWhenInWaitingState( Thread.currentThread() );

            try
            {
                assertThrows( ServiceUnavailableException.class, tx2::close );
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
        testFailWhenConnectionKilledDuringTransaction( false );
    }

    @Test
    void shouldThrowWhenConnectionKilledDuringTransactionMarkedForSuccess()
    {
        testFailWhenConnectionKilledDuringTransaction( true );
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
            assertThat( error2.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );

            ClientException error3 = assertThrows( ClientException.class, () -> tx.run( "RETURN 42" ).consume() );
            assertThat( error3.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );

            tx.success();
        }

        assertEquals( 0, countNodesByLabel( "Node" ) );
        assertEquals( 0, countNodesByLabel( "OtherNode" ) );
    }

    @Test
    void shouldRollbackWhenMarkedSuccessfulButOneStatementFails()
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

                tx.success();
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

    private static int countNodesByLabel( String label )
    {
        return session.run( "MATCH (n:" + label + ") RETURN count(n)" ).single().get( 0 ).asInt();
    }

    private static void testFailWhenConnectionKilledDuringTransaction( boolean markForSuccess )
    {
        ChannelTrackingDriverFactory factory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );
        Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).build();

        try ( Driver driver = factory.newInstance( session.uri(), session.authToken(), RoutingSettings.DEFAULT, DEFAULT, config ) )
        {
            ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () ->
            {
                try ( Session session = driver.session();
                      Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (:MyNode {id: 1})" ).consume();

                    if ( markForSuccess )
                    {
                        tx.success();
                    }

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
}
