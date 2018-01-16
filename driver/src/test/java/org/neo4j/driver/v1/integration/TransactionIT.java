/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1.integration;

import io.netty.channel.Channel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.util.ChannelTrackingDriverFactory;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.util.StubServer;
import org.neo4j.driver.v1.util.TestNeo4jSession;
import org.neo4j.driver.v1.util.TestUtil;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.retry.RetrySettings.DEFAULT;

public class TransactionIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldRunAndCommit() throws Throwable
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
    public void shouldRunAndRollbackByDefault() throws Throwable
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
    public void shouldRetrieveResults() throws Throwable
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
    public void shouldNotAllowSessionLevelStatementsWhenThereIsATransaction() throws Throwable
    {
        // Given
        session.beginTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        session.run( "anything" );
    }

    @Test
    public void shouldBeClosedAfterRollback() throws Throwable
    {
        // When
        Transaction tx = session.beginTransaction();
        tx.close();

        // Then
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeClosedAfterCommit() throws Throwable
    {
        // When
        Transaction tx = session.beginTransaction();
        tx.success();
        tx.close();

        // Then
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeOpenBeforeCommit() throws Throwable
    {
        // When
        Transaction tx = session.beginTransaction();

        // Then
        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldHandleNullParametersGracefully()
    {
        // When
        session.run( "match (n) return count(n)", (Value) null );

        // Then
        // pass - no exception thrown

    }

    //See GH #146
    @Test
    public void shouldHandleFailureAfterClosingTransaction()
    {
        // GIVEN a successful query in a transaction
        Transaction tx = session.beginTransaction();
        StatementResult result = tx.run( "CREATE (n) RETURN n" );
        result.consume();
        tx.success();
        tx.close();

        // EXPECT
        exception.expect( ClientException.class );

        //WHEN running a malformed query in the original session
        session.run( "CREAT (n) RETURN n" ).consume();
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldHandleNullRecordParameters() throws Throwable
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
    public void shouldHandleNullValueParameters() throws Throwable
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
    public void shouldHandleNullMapParameters() throws Throwable
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
    public void shouldRollBackTxIfErrorWithoutConsume() throws Throwable
    {
        // Given
        Transaction tx = session.beginTransaction();
        tx.run( "invalid" ); // send run, pull_all
        tx.success();
        try
        {
            tx.close();
            fail("should fail tx in tx.close()");
        } // When send run_commit, and pull_all

        // Then error and should also send ack_fail, roll_back and pull_all
        catch ( ClientException e )
        {
            try ( Transaction anotherTx = session.beginTransaction() )
            {
                StatementResult cursor = anotherTx.run( "RETURN 1" );
                int val = cursor.single().get( "1" ).asInt();


                assertThat( val, equalTo( 1 ) );
            }
        }
    }

    @Test
    public void shouldRollBackTxIfErrorWithConsume() throws Throwable
    {

        // Given
        try ( Transaction tx = session.beginTransaction() )
        {
            StatementResult result = tx.run( "invalid" );
            tx.success();

            // When
            result.consume(); // run, pull_all
            fail( "Should fail tx due to syntax error" );
        } // ack_fail, roll_back, pull_all
        // Then
        catch ( ClientException e )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                StatementResult cursor = tx.run( "RETURN 1" );
                int val = cursor.single().get( "1" ).asInt();

                assertThat( val, equalTo( 1 ) );
            }
        }
    }

    @Test
    public void shouldPropagateFailureFromSummary()
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            StatementResult result = tx.run( "RETURN Wrong" );

            try
            {
                result.summary();
                fail( "Exception expected" );
            }
            catch ( ClientException e )
            {
                assertThat( e.code(), containsString( "SyntaxError" ) );
            }

            assertNotNull( result.summary() );
        }
    }

    @Test
    public void shouldBeResponsiveToThreadInterruptWhenWaitingForResult() throws Exception
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
                tx2.run( "MATCH (n:Person {name: 'Beta Ray Bill'}) SET n.hammer = 'Stormbreaker'" ).consume();
                fail( "Exception expected" );
            }
            catch ( ServiceUnavailableException e )
            {
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
    public void shouldBeResponsiveToThreadInterruptWhenWaitingForCommit() throws Exception
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
                tx2.close();
                fail( "Exception expected" );
            }
            catch ( ServiceUnavailableException e )
            {
                assertEquals( e.getMessage(),
                        "Connection to the database terminated. Thread interrupted while closing the transaction" );
            }
            finally
            {
                // clear interrupted flag
                Thread.interrupted();
            }
        }
    }

    @Test
    public void shouldThrowWhenConnectionKilledDuringTransaction()
    {
        testFailWhenConnectionKilledDuringTransaction( false );
    }

    @Test
    public void shouldThrowWhenConnectionKilledDuringTransactionMarkedForSuccess()
    {
        testFailWhenConnectionKilledDuringTransaction( true );
    }

    @Test
    public void shouldThrowCommitError() throws Exception
    {
        testTxCloseErrorPropagation( "commit_error.script", true, "Unable to commit" );
    }

    @Test
    public void shouldThrowRollbackError() throws Exception
    {
        testTxCloseErrorPropagation( "rollback_error.script", false, "Unable to rollback" );
    }

    private void testFailWhenConnectionKilledDuringTransaction( boolean markForSuccess )
    {
        ChannelTrackingDriverFactory factory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );
        RoutingSettings instance = new RoutingSettings( 1, 0 );
        Config config = Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();

        try ( Driver driver = factory.newInstance( session.uri(), session.authToken(), instance, DEFAULT, config ) )
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
                for ( Channel channel : factory.channels() )
                {
                    channel.close().syncUninterruptibly();
                }

                tx.run( "CREATE (:MyNode {id: 1})" ).consume();
                fail( "Exception expected" );
            }
            catch ( ServiceUnavailableException e )
            {
                assertThat( e.getMessage(), containsString( "Connection to the database terminated" ) );
            }
        }

        assertEquals( 0, session.run( "MATCH (n:MyNode {id: 1}) RETURN count(n)" ).single().get( 0 ).asInt() );
    }

    private static void testTxCloseErrorPropagation( String script, boolean commit, String expectedErrorMessage )
            throws Exception
    {
        StubServer server = StubServer.start( script, 9001 );
        try
        {
            Config config = Config.build().withLogging( DEV_NULL_LOGGING ).withoutEncryption().toConfig();
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", AuthTokens.none(), config );
                  Session session = driver.session() )
            {
                Transaction tx = session.beginTransaction();
                StatementResult result = tx.run( "CREATE (n {name:'Alice'}) RETURN n.name AS name" );
                assertEquals( "Alice", result.single().get( "name" ).asString() );

                if ( commit )
                {
                    tx.success();
                }
                else
                {
                    tx.failure();
                }

                try
                {
                    tx.close();
                    fail( "Exception expected" );
                }
                catch ( TransientException e )
                {
                    assertEquals( "Neo.TransientError.General.DatabaseUnavailable", e.code() );
                    assertEquals( expectedErrorMessage, e.getMessage() );
                }
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }
}
