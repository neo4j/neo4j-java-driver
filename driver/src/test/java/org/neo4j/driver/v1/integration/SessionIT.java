/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.v1.Values.parameters;

public class SessionIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldKnowSessionIsClosed() throws Throwable
    {
        // Given
        try( Driver driver =  GraphDatabase.driver( neo4j.uri() ) )
        {
            Session session = driver.session();

            // When
            session.close();

            // Then
            assertFalse( session.isOpen() );
        }
    }

    @Test
    public void shouldHandleNullConfig() throws Throwable
    {
        // Given
        try( Driver driver = GraphDatabase.driver( neo4j.uri(), AuthTokens.none(), null ) )
        {
            Session session = driver.session();

            // When
            session.close();

            // Then
            assertFalse( session.isOpen() );
        }
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldHandleNullAuthToken() throws Throwable
    {
        // Given
        AuthToken token = null;
        try ( Driver driver = GraphDatabase.driver( neo4j.uri(), token) )
        {
            Session session = driver.session();

            // When
            session.close();

            // Then
            assertFalse( session.isOpen() );
        }
    }

    @Test
    public void shouldKillLongRunningStatement() throws Throwable
    {
        neo4j.ensureProcedures( "longRunningStatement.jar" );
        // Given
        Driver driver = GraphDatabase.driver( neo4j.uri() );

        int executionTimeout = 10; // 10s
        final int killTimeout = 1; // 1s
        long startTime = -1, endTime;

        try ( Session session = driver.session() )
        {
            StatementResult result =
                    session.run( "CALL test.driver.longRunningStatement({seconds})",
                            parameters( "seconds", executionTimeout ) );

            resetSessionAfterTimeout( session, killTimeout );

            // When
            startTime = System.currentTimeMillis();
            result.consume();// blocking to run the statement

            fail( "Should have got an exception about statement get killed." );
        }
        catch ( Neo4jException e )
        {
            endTime = System.currentTimeMillis();
            assertTrue( startTime > 0 );
            assertTrue( endTime - startTime > killTimeout * 1000 ); // get reset by session.reset
            assertTrue( endTime - startTime < executionTimeout * 1000 / 2 ); // finished before execution finished
        }
        catch ( Exception e )
        {
            fail( "Should be a Neo4jException" );
        }
    }

    @Test
    public void shouldKillLongStreamingResult() throws Throwable
    {
        neo4j.ensureProcedures( "longRunningStatement.jar" );
        // Given
        Driver driver = GraphDatabase.driver( neo4j.uri() );

        int executionTimeout = 10; // 10s
        final int killTimeout = 1; // 1s
        long startTime = -1, endTime;
        int recordCount = 0;

        try( final Session session = driver.session() )
        {
            StatementResult result = session.run( "CALL test.driver.longStreamingResult({seconds})",
                    parameters( "seconds", executionTimeout ) );

            resetSessionAfterTimeout( session, killTimeout );

            // When
            startTime = System.currentTimeMillis();
            while( result.hasNext() )
            {
                result.next();
                recordCount++;
            }

            fail("Should have got an exception about streaming get killed.");
        }
        catch( ClientException e )
        {
            endTime = System.currentTimeMillis();
            assertThat( e.code(), equalTo("Neo.ClientError.Procedure.ProcedureCallFailed") );
            assertThat( recordCount, greaterThan(1) );

            assertTrue( startTime > 0 );
            assertTrue( endTime - startTime > killTimeout * 1000 ); // get reset by session.reset
            assertTrue( endTime - startTime < executionTimeout * 1000 / 2 ); // finished before execution finished
        }
    }

    @Test
    public void shouldNotAllowBeginTxIfResetFailureIsNotConsumed() throws Throwable
    {
        // Given
        neo4j.ensureProcedures( "longRunningStatement.jar" );
        Driver driver = GraphDatabase.driver( neo4j.uri() );

        try( Session session = driver.session() )
        {
            Transaction tx = session.beginTransaction();

            tx.run("CALL test.driver.longRunningStatement({seconds})",
                    parameters( "seconds", 10 ) );
            Thread.sleep( 1000 );
            session.reset();

            exception.expect( ClientException.class );
            exception.expectMessage( startsWith(
                    "An error has occurred due to the cancellation of executing a previous statement." ) );

            // When & Then
            tx = session.beginTransaction();
            assertThat( tx, notNullValue() );
        }
    }

    @Test
    public void shouldThrowExceptionOnCloseIfResetFailureIsNotConsumed() throws Throwable
    {
        // Given
        neo4j.ensureProcedures( "longRunningStatement.jar" );
        Driver driver = GraphDatabase.driver( neo4j.uri() );

        Session session = driver.session();
        session.run( "CALL test.driver.longRunningStatement({seconds})",
                parameters( "seconds", 10 ) );
        Thread.sleep( 1000 );
        session.reset();

        exception.expect( ClientException.class );
        exception.expectMessage( startsWith(
                "An error has occurred due to the cancellation of executing a previous statement." ) );

        // When & Then
        session.close();
    }

    @Test
    public void shouldBeAbleToBeginTxAfterResetFailureIsConsumed() throws Throwable
    {
        // Given
        neo4j.ensureProcedures( "longRunningStatement.jar" );
        Driver driver = GraphDatabase.driver( neo4j.uri() );

        try( Session session = driver.session() )
        {
            Transaction tx = session.beginTransaction();

            StatementResult procedureResult = tx.run("CALL test.driver.longRunningStatement({seconds})",
                    parameters( "seconds", 10 ) );
            Thread.sleep( 1000 );
            session.reset();

            try
            {
                procedureResult.consume();
                fail( "Should procedure throw an exception as we interrupted procedure call" );
            }
            catch ( Neo4jException e )
            {
                assertThat( e.getMessage(), containsString( "The transaction has been terminated" ) );
            }
            catch ( Throwable e )
            {
                fail( "Expected exception is different from what we've received: " + e.getMessage() );
            }

            // When
            tx = session.beginTransaction();
            tx.run( "CREATE (n:FirstNode)" );
            tx.success();
            tx.close();

            // Then
            StatementResult result = session.run( "MATCH (n) RETURN count(n)" );
            long nodes = result.single().get( "count(n)" ).asLong();
            MatcherAssert.assertThat( nodes, equalTo( 1L ) );
        }
    }

    private void resetSessionAfterTimeout( final Session session, final int timeout )
    {
        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Thread.sleep( timeout * 1000 ); // let the statement executing for timeout seconds
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
                finally
                {
                    session.reset(); // reset the session after timeout
                }
            }
        } ).start();
    }

    @Test
    public void shouldAllowMoreStatementAfterSessionReset()
    {
        // Given
        try( Driver driver =  GraphDatabase.driver( neo4j.uri() );
             Session session = driver.session() )
        {

            session.run( "Return 1" ).consume();

            // When reset the state of this session
            session.reset();

            // Then can run successfully more statements without any error
            session.run( "Return 2" ).consume();
        }
    }

    @Test
    public void shouldAllowMoreTxAfterSessionReset()
    {
        // Given
        try( Driver driver =  GraphDatabase.driver( neo4j.uri() );
             Session session = driver.session() )
        {
            try( Transaction tx = session.beginTransaction() )
            {
                tx.run("Return 1");
                tx.success();
            }

            // When reset the state of this session
            session.reset();

            // Then can run more Tx
            try( Transaction tx = session.beginTransaction() )
            {
                tx.run("Return 2");
                tx.success();
            }
        }
    }

    @Test
    public void shouldMarkTxAsFailedAndDisallowRunAfterSessionReset()
    {
        // Given
        try( Driver driver =  GraphDatabase.driver( neo4j.uri() );
             Session session = driver.session() )
        {
            try( Transaction tx = session.beginTransaction() )
            {
                // When reset the state of this session
                session.reset();
                 // Then
                tx.run( "Return 1" );
                fail( "Should not allow tx run as tx is already failed." );
            }
            catch( Exception e )
            {
                assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );
            }
        }
    }

    @Test
    public void shouldAllowMoreTxAfterSessionResetInTx()
    {
        // Given
        try( Driver driver =  GraphDatabase.driver( neo4j.uri() );
             Session session = driver.session() )
        {
            try( Transaction tx = session.beginTransaction() )
            {
                // When reset the state of this session
                session.reset();
            }

            // Then can run more Tx
            try( Transaction tx = session.beginTransaction() )
            {
                tx.run("Return 2");
                tx.success();
            }
        }
    }

    @Test
    public void executeReadTxInReadSession()
    {
        testExecuteReadTx( AccessMode.READ );
    }

    @Test
    public void executeReadTxInWriteSession()
    {
        testExecuteReadTx( AccessMode.WRITE );
    }

    @Test
    public void executeWriteTxInReadSession()
    {
        testExecuteWriteTx( AccessMode.READ );
    }

    @Test
    public void executeWriteTxInWriteSession()
    {
        testExecuteWriteTx( AccessMode.WRITE );
    }

    @Test
    public void rollsBackWriteTxInReadSessionWhenFunctionThrows()
    {
        testTxRollbackWhenFunctionThrows( AccessMode.READ );
    }

    @Test
    public void rollsBackWriteTxInWriteSessionWhenFunctionThrows()
    {
        testTxRollbackWhenFunctionThrows( AccessMode.WRITE );
    }

    private void testExecuteReadTx( AccessMode sessionMode )
    {
        Driver driver = neo4j.driver();

        // write some test data
        try ( Session session = driver.session() )
        {
            session.run( "CREATE (:Person {name: 'Tony Stark'})" );
            session.run( "CREATE (:Person {name: 'Steve Rogers'})" );
        }

        // read previously committed data
        try ( Session session = driver.session( sessionMode ) )
        {
            Set<String> names = session.readTransaction( new Function<Transaction,Set<String>>()
            {
                @Override
                public Set<String> apply( Transaction tx )
                {
                    List<Record> records = tx.run( "MATCH (p:Person) RETURN p.name AS name" ).list();
                    Set<String> names = new HashSet<>( records.size() );
                    for ( Record record : records )
                    {
                        names.add( record.get( "name" ).asString() );
                    }
                    return names;
                }
            } );

            assertThat( names, containsInAnyOrder( "Tony Stark", "Steve Rogers" ) );
        }
    }

    private void testExecuteWriteTx( AccessMode sessionMode )
    {
        Driver driver = neo4j.driver();

        // write some test data
        try ( Session session = driver.session( sessionMode ) )
        {
            String material = session.writeTransaction( new Function<Transaction,String>()
            {
                @Override
                public String apply( Transaction tx )
                {
                    StatementResult result = tx.run( "CREATE (s:Shield {material: 'Vibranium'}) RETURN s" );
                    tx.success();
                    Record record = result.single();
                    return record.get( 0 ).asNode().get( "material" ).asString();
                }
            } );

            assertEquals( "Vibranium", material );
        }

        // read previously committed data
        try ( Session session = driver.session() )
        {
            Record record = session.run( "MATCH (s:Shield) RETURN s.material" ).single();
            assertEquals( "Vibranium", record.get( 0 ).asString() );
        }
    }

    private void testTxRollbackWhenFunctionThrows( AccessMode sessionMode )
    {
        Driver driver = neo4j.driver();

        try ( Session session = driver.session( sessionMode ) )
        {
            try
            {
                session.writeTransaction( new Function<Transaction,Void>()
                {
                    @Override
                    public Void apply( Transaction tx )
                    {
                        tx.run( "CREATE (:Person {name: 'Thanos'})" );
                        // trigger division by zero error:
                        tx.run( "UNWIND range(0, 1) AS i RETURN 10/i" );
                        tx.success();
                        return null;
                    }
                } );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ClientException.class ) );
            }
        }

        // no data should have been committed
        try ( Session session = driver.session() )
        {
            Record record = session.run( "MATCH (p:Person {name: 'Thanos'}) RETURN count(p)" ).single();
            assertEquals( 0, record.get( 0 ).asInt() );
        }
    }
}
