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

import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Response;
import org.neo4j.driver.v1.ResponseListener;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.TestUtil.await;
import static org.neo4j.driver.v1.util.TestUtil.awaitAll;

public class SessionAsyncIT
{
    @Rule
    public final TestNeo4j neo4j = new TestNeo4j();

    private Session session;

    @Before
    public void setUp() throws Exception
    {
        session = neo4j.driver().session();
    }

    @After
    public void tearDown() throws Exception
    {
        await( session.closeAsync() );
    }

    @Test
    public void shouldRunQueryWithEmptyResult()
    {
        StatementResultCursor cursor = await( session.runAsync( "CREATE (:Person)" ) );

        assertThat( await( cursor.fetchAsync() ), is( false ) );
    }

    @Test
    public void shouldRunQueryWithSingleResult()
    {
        StatementResultCursor cursor = await( session.runAsync( "CREATE (p:Person {name: 'Nick Fury'}) RETURN p" ) );

        assertThat( await( cursor.fetchAsync() ), is( true ) );

        Record record = cursor.current();
        Node node = record.get( 0 ).asNode();
        assertEquals( "Person", single( node.labels() ) );
        assertEquals( "Nick Fury", node.get( "name" ).asString() );

        assertThat( await( cursor.fetchAsync() ), is( false ) );
    }

    @Test
    public void shouldRunQueryWithMultipleResults()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND [1,2,3] AS x RETURN x" ) );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 1, cursor.current().get( 0 ).asInt() );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 2, cursor.current().get( 0 ).asInt() );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 3, cursor.current().get( 0 ).asInt() );

        assertThat( await( cursor.fetchAsync() ), is( false ) );
    }

    @Test
    public void shouldFailForIncorrectQuery()
    {
        try
        {
            await( session.runAsync( "RETURN" ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertSyntaxError( e );
        }
    }

    @Test
    public void shouldFailWhenQueryFailsAtRuntime()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND [1, 2, 0] AS x RETURN 10 / x" ) );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 10, cursor.current().get( 0 ).asInt() );

        assertThat( await( cursor.fetchAsync() ), is( true ) );
        assertEquals( 5, cursor.current().get( 0 ).asInt() );

        try
        {
            await( cursor.fetchAsync() );
            System.out.println( cursor.current() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertArithmeticError( e );
        }
    }

    @Test
    public void shouldFailWhenServerIsRestarted() throws Exception
    {
        StatementResultCursor cursor = await( session.runAsync(
                "UNWIND range(0, 1000000) AS x " +
                "CREATE (n1:Node {value: x})-[r:LINKED {value: x}]->(n2:Node {value: x}) " +
                "DETACH DELETE n1, n2 " +
                "RETURN x" ) );

        try
        {
            Response<Boolean> recordAvailable = cursor.fetchAsync();

            // kill db after receiving the first record
            // do it from a listener so that event loop thread executes the kill operation
            recordAvailable.addListener( new KillDbListener( neo4j ) );

            while ( await( recordAvailable ) )
            {
                assertNotNull( cursor.current() );
                recordAvailable = cursor.fetchAsync();
            }
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        }
    }

    @Test
    public void shouldAllowNestedQueries()
    {
        StatementResultCursor cursor =
                await( session.runAsync( "UNWIND [1, 2, 3] AS x CREATE (p:Person {id: x}) RETURN p" ) );

        Future<List<Future<Boolean>>> queriesExecuted = runNestedQueries( cursor );
        List<Future<Boolean>> futures = await( queriesExecuted );

        List<Boolean> futureResults = awaitAll( futures );
        assertEquals( 7, futureResults.size() );

        StatementResultCursor personCursor = await( session.runAsync( "MATCH (p:Person) RETURN p ORDER BY p.id" ) );

        List<Node> personNodes = new ArrayList<>();
        while ( await( personCursor.fetchAsync() ) )
        {
            personNodes.add( personCursor.current().get( 0 ).asNode() );
        }
        assertEquals( 3, personNodes.size() );

        Node node1 = personNodes.get( 0 );
        assertEquals( 1, node1.get( "id" ).asInt() );
        assertEquals( 10, node1.get( "age" ).asInt() );

        Node node2 = personNodes.get( 1 );
        assertEquals( 2, node2.get( "id" ).asInt() );
        assertEquals( 20, node2.get( "age" ).asInt() );

        Node node3 = personNodes.get( 2 );
        assertEquals( 3, node3.get( "id" ).asInt() );
        assertEquals( 30, personNodes.get( 2 ).get( "age" ).asInt() );
    }

    @Test
    public void shouldAllowMultipleAsyncRunsWithoutConsumingResults() throws InterruptedException
    {
        int queryCount = 13;
        List<Future<StatementResultCursor>> cursors = new ArrayList<>();
        for ( int i = 0; i < queryCount; i++ )
        {
            cursors.add( session.runAsync( "CREATE (:Person)" ) );
        }

        List<Future<Boolean>> fetches = new ArrayList<>();
        for ( StatementResultCursor cursor : awaitAll( cursors ) )
        {
            fetches.add( cursor.fetchAsync() );
        }

        awaitAll( fetches );

        await( session.closeAsync() );
        session = neo4j.driver().session();

        StatementResultCursor cursor = await( session.runAsync( "MATCH (p:Person) RETURN count(p)" ) );
        assertThat( await( cursor.fetchAsync() ), is( true ) );

        Record record = cursor.current();
        assertEquals( queryCount, record.get( 0 ).asInt() );
    }

    @Test
    public void shouldExposeStatementKeysForColumnsWithAliases()
    {
        StatementResultCursor cursor = await( session.runAsync( "RETURN 1 AS one, 2 AS two, 3 AS three, 4 AS five" ) );

        assertEquals( Arrays.asList( "one", "two", "three", "five" ), cursor.keys() );
    }

    @Test
    public void shouldExposeStatementKeysForColumnsWithoutAliases()
    {
        StatementResultCursor cursor = await( session.runAsync( "RETURN 1, 2, 3, 5" ) );

        assertEquals( Arrays.asList( "1", "2", "3", "5" ), cursor.keys() );
    }

    @Test
    public void shouldExposeResultSummaryForSimpleQuery()
    {
        String query = "CREATE (:Node {id: $id, name: $name})";
        Value params = parameters( "id", 1, "name", "TheNode" );

        StatementResultCursor cursor = await( session.runAsync( query, params ) );
        ResultSummary summary = await( cursor.summaryAsync() );

        assertEquals( new Statement( query, params ), summary.statement() );
        assertEquals( 1, summary.counters().nodesCreated() );
        assertEquals( 1, summary.counters().labelsAdded() );
        assertEquals( 2, summary.counters().propertiesSet() );
        assertEquals( 0, summary.counters().relationshipsCreated() );
        assertEquals( StatementType.WRITE_ONLY, summary.statementType() );
        assertFalse( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertNull( summary.plan() );
        assertNull( summary.profile() );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary.resultAvailableAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
        assertThat( summary.resultConsumedAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
    }

    @Test
    public void shouldExposeResultSummaryForExplainQuery()
    {
        String query = "EXPLAIN CREATE (),() WITH * MATCH (n)-->(m) CREATE (n)-[:HI {id: 'id'}]->(m) RETURN n, m";

        StatementResultCursor cursor = await( session.runAsync( query ) );
        ResultSummary summary = await( cursor.summaryAsync() );

        assertEquals( new Statement( query ), summary.statement() );
        assertEquals( 0, summary.counters().nodesCreated() );
        assertEquals( 0, summary.counters().propertiesSet() );
        assertEquals( 0, summary.counters().relationshipsCreated() );
        assertEquals( StatementType.READ_WRITE, summary.statementType() );
        assertTrue( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertNotNull( summary.plan() );
        // asserting on plan is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        String planAsString = summary.plan().toString();
        assertThat( planAsString, containsString( "CreateNode" ) );
        assertThat( planAsString, containsString( "Expand" ) );
        assertThat( planAsString, containsString( "AllNodesScan" ) );
        assertNull( summary.profile() );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary.resultAvailableAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
        assertThat( summary.resultConsumedAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
    }

    @Test
    public void shouldExposeResultSummaryForProfileQuery()
    {
        String query = "PROFILE CREATE (:Node)-[:KNOWS]->(:Node) WITH * MATCH (n) RETURN n";

        StatementResultCursor cursor = await( session.runAsync( query ) );
        ResultSummary summary = await( cursor.summaryAsync() );

        assertEquals( new Statement( query ), summary.statement() );
        assertEquals( 2, summary.counters().nodesCreated() );
        assertEquals( 0, summary.counters().propertiesSet() );
        assertEquals( 1, summary.counters().relationshipsCreated() );
        assertEquals( StatementType.READ_WRITE, summary.statementType() );
        assertTrue( summary.hasPlan() );
        assertTrue( summary.hasProfile() );
        assertNotNull( summary.plan() );
        assertNotNull( summary.profile() );
        // asserting on profile is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        String profileAsString = summary.profile().toString();
        assertThat( profileAsString, containsString( "DbHits" ) );
        assertThat( profileAsString, containsString( "PageCacheHits" ) );
        assertThat( profileAsString, containsString( "CreateNode" ) );
        assertThat( profileAsString, containsString( "CreateRelationship" ) );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary.resultAvailableAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
        assertThat( summary.resultConsumedAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
    }

    private Future<List<Future<Boolean>>> runNestedQueries( StatementResultCursor inputCursor )
    {
        Promise<List<Future<Boolean>>> resultPromise = GlobalEventExecutor.INSTANCE.newPromise();
        runNestedQueries( inputCursor, new ArrayList<Future<Boolean>>(), resultPromise );
        return resultPromise;
    }

    private void runNestedQueries( final StatementResultCursor inputCursor, final List<Future<Boolean>> futures,
            final Promise<List<Future<Boolean>>> resultPromise )
    {
        final Response<Boolean> inputAvailable = inputCursor.fetchAsync();
        futures.add( inputAvailable );

        inputAvailable.addListener( new ResponseListener<Boolean>()
        {
            @Override
            public void operationCompleted( Boolean inputAvailable, Throwable error )
            {
                if ( error != null )
                {
                    resultPromise.setFailure( error );
                }
                else if ( inputAvailable )
                {
                    runNestedQuery( inputCursor, futures, resultPromise );
                }
                else
                {
                    resultPromise.setSuccess( futures );
                }
            }
        } );
    }

    private void runNestedQuery( final StatementResultCursor inputCursor, final List<Future<Boolean>> futures,
            final Promise<List<Future<Boolean>>> resultPromise )
    {
        Record record = inputCursor.current();
        Node node = record.get( 0 ).asNode();
        long id = node.get( "id" ).asLong();
        long age = id * 10;

        Response<StatementResultCursor> response =
                session.runAsync( "MATCH (p:Person {id: $id}) SET p.age = $age RETURN p",
                parameters( "id", id, "age", age ) );

        response.addListener( new ResponseListener<StatementResultCursor>()
        {
            @Override
            public void operationCompleted( StatementResultCursor result, Throwable error )
            {
                if ( error != null )
                {
                    resultPromise.setFailure( error );
                }
                else
                {
                    futures.add( result.fetchAsync() );
                    runNestedQueries( inputCursor, futures, resultPromise );
                }
            }
        } );
    }

    private static void assertSyntaxError( Exception e )
    {
        assertThat( e, instanceOf( ClientException.class ) );
        assertThat( ((ClientException) e).code(), containsString( "SyntaxError" ) );
        assertThat( e.getMessage(), startsWith( "Unexpected end of input" ) );
    }

    private static void assertArithmeticError( Exception e )
    {
        assertThat( e, instanceOf( ClientException.class ) );
        assertThat( ((ClientException) e).code(), containsString( "ArithmeticError" ) );
    }

    private static class KillDbListener implements ResponseListener<Boolean>
    {
        final TestNeo4j neo4j;
        volatile boolean shouldKillDb = true;

        KillDbListener( TestNeo4j neo4j )
        {
            this.neo4j = neo4j;
        }

        @Override
        public void operationCompleted( Boolean result, Throwable error )
        {
            if ( shouldKillDb )
            {
                killDb();
                shouldKillDb = false;
            }
        }

        void killDb()
        {
            try
            {
                neo4j.killDb();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
