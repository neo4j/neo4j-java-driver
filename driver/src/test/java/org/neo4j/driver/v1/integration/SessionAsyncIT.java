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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.containsString;
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
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.internal.util.Matchers.arithmeticError;
import static org.neo4j.driver.internal.util.Matchers.blockingOperationInEventLoopError;
import static org.neo4j.driver.internal.util.Matchers.containsResultAvailableAfterAndResultConsumedAfter;
import static org.neo4j.driver.internal.util.Matchers.syntaxError;
import static org.neo4j.driver.internal.util.ServerVersion.v3_1_0;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.TestUtil.await;
import static org.neo4j.driver.v1.util.TestUtil.awaitAll;

public class SessionAsyncIT
{
    private final TestNeo4j neo4j = new TestNeo4j();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( Timeout.seconds( 180 ) ).around( neo4j );

    private Session session;

    @Before
    public void setUp()
    {
        session = neo4j.driver().session();
    }

    @After
    public void tearDown()
    {
        session.closeAsync();
    }

    @Test
    public void shouldRunQueryWithEmptyResult()
    {
        StatementResultCursor cursor = await( session.runAsync( "CREATE (:Person)" ) );

        assertNull( await( cursor.nextAsync() ) );
    }

    @Test
    public void shouldRunQueryWithSingleResult()
    {
        StatementResultCursor cursor = await( session.runAsync( "CREATE (p:Person {name: 'Nick Fury'}) RETURN p" ) );

        Record record = await( cursor.nextAsync() );
        assertNotNull( record );
        Node node = record.get( 0 ).asNode();
        assertEquals( "Person", single( node.labels() ) );
        assertEquals( "Nick Fury", node.get( "name" ).asString() );

        assertNull( await( cursor.nextAsync() ) );
    }

    @Test
    public void shouldRunQueryWithMultipleResults()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND [1,2,3] AS x RETURN x" ) );

        Record record1 = await( cursor.nextAsync() );
        assertNotNull( record1 );
        assertEquals( 1, record1.get( 0 ).asInt() );

        Record record2 = await( cursor.nextAsync() );
        assertNotNull( record2 );
        assertEquals( 2, record2.get( 0 ).asInt() );

        Record record3 = await( cursor.nextAsync() );
        assertNotNull( record3 );
        assertEquals( 3, record3.get( 0 ).asInt() );

        assertNull( await( cursor.nextAsync() ) );
    }

    @Test
    public void shouldFailForIncorrectQuery()
    {
        StatementResultCursor cursor = await( session.runAsync( "RETURN" ) );
        try
        {
            await( cursor.nextAsync() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        }
    }

    @Test
    public void shouldFailWhenQueryFailsAtRuntime()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND [1, 2, 0] AS x RETURN 10 / x" ) );

        Record record1 = await( cursor.nextAsync() );
        assertNotNull( record1 );
        assertEquals( 10, record1.get( 0 ).asInt() );

        Record record2 = await( cursor.nextAsync() );
        assertNotNull( record2 );
        assertEquals( 5, record2.get( 0 ).asInt() );

        try
        {
            await( cursor.nextAsync() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( arithmeticError() ) );
        }
    }

    @Test
    public void shouldFailWhenServerIsRestarted()
    {
        int queryCount = 10_000;

        String query = "UNWIND range(1, 100) AS x " +
                       "CREATE (n1:Node {value: x})-[r:LINKED {value: x}]->(n2:Node {value: x}) " +
                       "DETACH DELETE n1, n2 " +
                       "RETURN x";

        try
        {
            for ( int i = 0; i < queryCount; i++ )
            {
                StatementResultCursor cursor = await( session.runAsync( query ) );

                if ( i == 0 )
                {
                    neo4j.killDb();
                }

                List<Record> records = await( cursor.listAsync() );
                assertEquals( 100, records.size() );
            }
            fail( "Exception expected" );
        }
        catch ( Throwable t )
        {
            assertThat( t, instanceOf( ServiceUnavailableException.class ) );
        }
    }

    @Test
    public void shouldAllowNestedQueries()
    {
        StatementResultCursor cursor =
                await( session.runAsync( "UNWIND [1, 2, 3] AS x CREATE (p:Person {id: x}) RETURN p" ) );

        Future<List<CompletionStage<Record>>> queriesExecuted = runNestedQueries( cursor );
        List<CompletionStage<Record>> futures = await( queriesExecuted );

        List<Record> futureResults = awaitAll( futures );
        assertEquals( 7, futureResults.size() );

        StatementResultCursor personCursor = await( session.runAsync( "MATCH (p:Person) RETURN p ORDER BY p.id" ) );

        List<Node> personNodes = new ArrayList<>();
        Record record;
        while ( (record = await( personCursor.nextAsync() )) != null )
        {
            personNodes.add( record.get( 0 ).asNode() );
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
    public void shouldAllowMultipleAsyncRunsWithoutConsumingResults()
    {
        int queryCount = 13;
        List<CompletionStage<StatementResultCursor>> cursors = new ArrayList<>();
        for ( int i = 0; i < queryCount; i++ )
        {
            cursors.add( session.runAsync( "CREATE (:Person)" ) );
        }

        List<CompletionStage<Record>> records = new ArrayList<>();
        for ( StatementResultCursor cursor : awaitAll( cursors ) )
        {
            records.add( cursor.nextAsync() );
        }

        awaitAll( records );

        await( session.closeAsync() );
        session = neo4j.driver().session();

        StatementResultCursor cursor = await( session.runAsync( "MATCH (p:Person) RETURN count(p)" ) );
        Record record = await( cursor.nextAsync() );
        assertNotNull( record );
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
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
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
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
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
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    public void shouldRunAsyncTransactionWithoutRetries()
    {
        InvocationTrackingWork work = new InvocationTrackingWork( "CREATE (:Apa) RETURN 42" );
        CompletionStage<Record> txStage = session.writeTransactionAsync( work );

        Record record = await( txStage );
        assertNotNull( record );
        assertEquals( 42L, record.get( 0 ).asLong() );

        assertEquals( 1, work.invocationCount() );
        assertEquals( 1, countNodesByLabel( "Apa" ) );
    }

    @Test
    public void shouldRunAsyncTransactionWithRetriesOnAsyncFailures()
    {
        InvocationTrackingWork work = new InvocationTrackingWork( "CREATE (:Node) RETURN 24" ).withAsyncFailures(
                new ServiceUnavailableException( "Oh!" ),
                new SessionExpiredException( "Ah!" ),
                new TransientException( "Code", "Message" ) );

        CompletionStage<Record> txStage = session.writeTransactionAsync( work );

        Record record = await( txStage );
        assertNotNull( record );
        assertEquals( 24L, record.get( 0 ).asLong() );

        assertEquals( 4, work.invocationCount() );
        assertEquals( 1, countNodesByLabel( "Node" ) );
    }

    @Test
    public void shouldRunAsyncTransactionWithRetriesOnSyncFailures()
    {
        InvocationTrackingWork work = new InvocationTrackingWork( "CREATE (:Test) RETURN 12" ).withSyncFailures(
                new TransientException( "Oh!", "Deadlock!" ),
                new ServiceUnavailableException( "Oh! Network Failure" ) );

        CompletionStage<Record> txStage = session.writeTransactionAsync( work );

        Record record = await( txStage );
        assertNotNull( record );
        assertEquals( 12L, record.get( 0 ).asLong() );

        assertEquals( 3, work.invocationCount() );
        assertEquals( 1, countNodesByLabel( "Test" ) );
    }

    @Test
    public void shouldRunAsyncTransactionThatCanNotBeRetried()
    {
        InvocationTrackingWork work = new InvocationTrackingWork( "UNWIND [10, 5, 0] AS x CREATE (:Hi) RETURN 10/x" );
        CompletionStage<Record> txStage = session.writeTransactionAsync( work );

        try
        {
            await( txStage );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }

        assertEquals( 1, work.invocationCount() );
        assertEquals( 0, countNodesByLabel( "Hi" ) );
    }

    @Test
    public void shouldRunAsyncTransactionThatCanNotBeRetriedAfterATransientFailure()
    {
        // first throw TransientException directly from work, retry can happen afterwards
        // then return a future failed with DatabaseException, retry can't happen afterwards
        InvocationTrackingWork work = new InvocationTrackingWork( "CREATE (:Person) RETURN 1" )
                .withSyncFailures( new TransientException( "Oh!", "Deadlock!" ) )
                .withAsyncFailures( new DatabaseException( "Oh!", "OutOfMemory!" ) );
        CompletionStage<Record> txStage = session.writeTransactionAsync( work );

        try
        {
            await( txStage );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( DatabaseException.class ) );
            assertEquals( 1, e.getSuppressed().length );
            assertThat( e.getSuppressed()[0], instanceOf( TransientException.class ) );
        }

        assertEquals( 2, work.invocationCount() );
        assertEquals( 0, countNodesByLabel( "Person" ) );
    }

    @Test
    public void shouldPeekRecordFromCursor()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND [1, 2, 42] AS x RETURN x" ) );

        assertEquals( 1, await( cursor.peekAsync() ).get( 0 ).asInt() );
        assertEquals( 1, await( cursor.peekAsync() ).get( 0 ).asInt() );
        assertEquals( 1, await( cursor.peekAsync() ).get( 0 ).asInt() );

        assertEquals( 1, await( cursor.nextAsync() ).get( 0 ).asInt() );

        assertEquals( 2, await( cursor.peekAsync() ).get( 0 ).asInt() );
        assertEquals( 2, await( cursor.peekAsync() ).get( 0 ).asInt() );

        assertEquals( 2, await( cursor.nextAsync() ).get( 0 ).asInt() );

        assertEquals( 42, await( cursor.nextAsync() ).get( 0 ).asInt() );

        assertNull( await( cursor.peekAsync() ) );
        assertNull( await( cursor.nextAsync() ) );
    }

    @Test
    public void shouldForEachWithEmptyCursor()
    {
        testForEach( "CREATE ()", 0 );
    }

    @Test
    public void shouldForEachWithNonEmptyCursor()
    {
        testForEach( "UNWIND range(1, 100000) AS x RETURN x", 100000 );
    }

    @Test
    public void shouldFailForEachWhenActionFails()
    {
        StatementResultCursor cursor = await( session.runAsync( "RETURN 42" ) );
        IOException error = new IOException( "Hi" );

        try
        {
            await( cursor.forEachAsync( record ->
            {
                throw new CompletionException( error );
            } ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertEquals( error, e );
        }
    }

    @Test
    public void shouldConvertToListWithEmptyCursor()
    {
        testList( "MATCH (n:NoSuchLabel) RETURN n", Collections.emptyList() );
    }

    @Test
    public void shouldConvertToListWithNonEmptyCursor()
    {
        testList( "UNWIND range(1, 100, 10) AS x RETURN x",
                Arrays.asList( 1L, 11L, 21L, 31L, 41L, 51L, 61L, 71L, 81L, 91L ) );
    }

    @Test
    public void shouldConvertToTransformedListWithEmptyCursor()
    {
        StatementResultCursor cursor = await( session.runAsync( "CREATE ()" ) );
        List<String> strings = await( cursor.listAsync( record -> "Hi!" ) );
        assertEquals( 0, strings.size() );
    }

    @Test
    public void shouldConvertToTransformedListWithNonEmptyCursor()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND [1,2,3] AS x RETURN x" ) );
        List<Integer> ints = await( cursor.listAsync( record -> record.get( 0 ).asInt() + 1 ) );
        assertEquals( Arrays.asList( 2, 3, 4 ), ints );
    }

    @Test
    public void shouldFailWhenListTransformationFunctionFails()
    {
        StatementResultCursor cursor = await( session.runAsync( "RETURN 42" ) );
        RuntimeException error = new RuntimeException( "Hi!" );

        try
        {
            await( cursor.listAsync( record ->
            {
                throw error;
            } ) );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( error, e );
        }
    }

    @Test
    public void shouldFailSingleWithEmptyCursor()
    {
        StatementResultCursor cursor = await( session.runAsync( "CREATE ()" ) );

        try
        {
            await( cursor.singleAsync() );
            fail( "Exception expected" );
        }
        catch ( NoSuchRecordException e )
        {
            assertThat( e.getMessage(), containsString( "cursor is empty" ) );
        }
    }

    @Test
    public void shouldFailSingleWithMultiRecordCursor()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND [1, 2, 3] AS x RETURN x" ) );

        try
        {
            await( cursor.singleAsync() );
            fail( "Exception expected" );
        }
        catch ( NoSuchRecordException e )
        {
            assertThat( e.getMessage(), startsWith( "Expected a result with a single record" ) );
        }
    }

    @Test
    public void shouldReturnSingleWithSingleRecordCursor()
    {
        StatementResultCursor cursor = await( session.runAsync( "RETURN 42" ) );

        Record record = await( cursor.singleAsync() );

        assertEquals( 42, record.get( 0 ).asInt() );
    }

    @Test
    public void shouldPropagateFailureFromFirstRecordInSingleAsync()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND [0] AS x RETURN 10 / x" ) );

        try
        {
            await( cursor.singleAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }
    }

    @Test
    public void shouldNotPropagateFailureFromSecondRecordInSingleAsync()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND [1, 0] AS x RETURN 10 / x" ) );

        try
        {
            await( cursor.singleAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }
    }

    @Test
    public void shouldConsumeEmptyCursor()
    {
        testConsume( "CREATE ()" );
    }

    @Test
    public void shouldConsumeNonEmptyCursor()
    {
        testConsume( "UNWIND [42, 42] AS x RETURN x" );
    }

    @Test
    public void shouldRunAfterRunFailureToAcquireConnection()
    {
        neo4j.killDb();

        try
        {
            StatementResultCursor cursor = await( session.runAsync( "RETURN 42" ) );
            await( cursor.nextAsync() );
            fail( "Exception expected" );
        }
        catch ( ServiceUnavailableException e )
        {
            // expected
        }

        neo4j.startDb();

        StatementResultCursor cursor2 = await( session.runAsync( "RETURN 42" ) );
        Record record = await( cursor2.singleAsync() );
        assertEquals( 42, record.get( 0 ).asInt() );
    }

    @Test
    public void shouldRunAfterBeginTxFailureOnBookmark()
    {
        ServerVersion version = neo4j.version();
        assumeTrue( "Server " + version + " does not support bookmark", version.greaterThanOrEqual( v3_1_0 ) );

        session = neo4j.driver().session( "Illegal Bookmark" );

        try
        {
            await( session.beginTransactionAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            // expected
        }

        StatementResultCursor cursor = await( session.runAsync( "RETURN 'Hello!'" ) );
        Record record = await( cursor.singleAsync() );
        assertEquals( "Hello!", record.get( 0 ).asString() );
    }

    @Test
    public void shouldBeginTxAfterRunFailureToAcquireConnection()
    {
        neo4j.killDb();

        try
        {
            StatementResultCursor cursor = await( session.runAsync( "RETURN 42" ) );
            await( cursor.consumeAsync() );
            fail( "Exception expected" );
        }
        catch ( ServiceUnavailableException e )
        {
            // expected
        }

        neo4j.startDb();

        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor2 = await( tx.runAsync( "RETURN 42" ) );
        Record record = await( cursor2.singleAsync() );
        assertEquals( 42, record.get( 0 ).asInt() );
        assertNull( await( tx.rollbackAsync() ) );
    }

    @Test
    public void shouldExecuteReadTransactionUntilSuccessWhenWorkThrows()
    {
        int maxFailures = 1;

        CompletionStage<Integer> result = session.readTransactionAsync( new TransactionWork<CompletionStage<Integer>>()
        {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<Integer> execute( Transaction tx )
            {
                if ( failures.getAndIncrement() < maxFailures )
                {
                    throw new SessionExpiredException( "Oh!" );
                }
                return tx.runAsync( "UNWIND range(1, 10) AS x RETURN count(x)" )
                        .thenCompose( StatementResultCursor::singleAsync )
                        .thenApply( record -> record.get( 0 ).asInt() );
            }
        } );

        assertEquals( 10, await( result ).intValue() );
    }

    @Test
    public void shouldExecuteWriteTransactionUntilSuccessWhenWorkThrows()
    {
        int maxFailures = 2;

        CompletionStage<Integer> result = session.writeTransactionAsync( new TransactionWork<CompletionStage<Integer>>()
        {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<Integer> execute( Transaction tx )
            {
                if ( failures.getAndIncrement() < maxFailures )
                {
                    throw new ServiceUnavailableException( "Oh!" );
                }
                return tx.runAsync( "CREATE (n1:TestNode), (n2:TestNode) RETURN 2" )
                        .thenCompose( StatementResultCursor::singleAsync )
                        .thenApply( record -> record.get( 0 ).asInt() );
            }
        } );

        assertEquals( 2, await( result ).intValue() );
        assertEquals( 2, countNodesByLabel( "TestNode" ) );
    }

    @Test
    public void shouldExecuteReadTransactionUntilSuccessWhenWorkFails()
    {
        int maxFailures = 3;

        CompletionStage<Integer> result = session.readTransactionAsync( new TransactionWork<CompletionStage<Integer>>()
        {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<Integer> execute( Transaction tx )
            {
                return tx.runAsync( "RETURN 42" )
                        .thenCompose( StatementResultCursor::singleAsync )
                        .thenApply( record -> record.get( 0 ).asInt() )
                        .thenCompose( result ->
                        {
                            if ( failures.getAndIncrement() < maxFailures )
                            {
                                return failedFuture( new TransientException( "A", "B" ) );
                            }
                            return completedFuture( result );
                        } );
            }
        } );

        assertEquals( 42, await( result ).intValue() );
    }

    @Test
    public void shouldExecuteWriteTransactionUntilSuccessWhenWorkFails()
    {
        int maxFailures = 2;

        CompletionStage<String> result = session.writeTransactionAsync( new TransactionWork<CompletionStage<String>>()
        {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<String> execute( Transaction tx )
            {
                return tx.runAsync( "CREATE (:MyNode) RETURN 'Hello'" )
                        .thenCompose( StatementResultCursor::singleAsync )
                        .thenApply( record -> record.get( 0 ).asString() )
                        .thenCompose( result ->
                        {
                            if ( failures.getAndIncrement() < maxFailures )
                            {
                                return failedFuture( new ServiceUnavailableException( "Hi" ) );
                            }
                            return completedFuture( result );
                        } );
            }
        } );

        assertEquals( "Hello", await( result ) );
        assertEquals( 1, countNodesByLabel( "MyNode" ) );
    }

    @Test
    public void shouldPropagateRunFailureWhenClosed()
    {
        session.runAsync( "RETURN 10 / 0" );

        try
        {
            await( session.closeAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }
    }

    @Test
    public void shouldPropagateBlockedRunFailureWhenClosed()
    {
        await( session.runAsync( "RETURN 10 / 0" ) );

        try
        {
            await( session.closeAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }
    }


    @Test
    public void shouldPropagatePullAllFailureWhenClosed()
    {
        session.runAsync( "UNWIND range(20000, 0, -1) AS x RETURN 10 / x" );

        try
        {
            await( session.closeAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }
    }

    @Test
    public void shouldPropagateBlockedPullAllFailureWhenClosed()
    {
        await( session.runAsync( "UNWIND range(20000, 0, -1) AS x RETURN 10 / x" ) );

        try
        {
            await( session.closeAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }
    }

    @Test
    public void shouldCloseCleanlyWhenRunErrorConsumed()
    {
        StatementResultCursor cursor = await( session.runAsync( "SomeWrongQuery" ) );

        try
        {
            await( cursor.consumeAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), startsWith( "Invalid input" ) );
        }

        assertNull( await( session.closeAsync() ) );
    }

    @Test
    public void shouldCloseCleanlyWhenPullAllErrorConsumed()
    {
        StatementResultCursor cursor = await( session.runAsync( "UNWIND range(10, 0, -1) AS x RETURN 1 / x" ) );

        try
        {
            await( cursor.consumeAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), containsString( "/ by zero" ) );
        }

        assertNull( await( session.closeAsync() ) );
    }

    @Test
    public void shouldBePossibleToConsumeResultAfterSessionIsClosed()
    {
        CompletionStage<StatementResultCursor> cursorStage = session.runAsync( "UNWIND range(1, 20000) AS x RETURN x" );

        await( session.closeAsync() );

        StatementResultCursor cursor = await( cursorStage );
        List<Integer> ints = await( cursor.listAsync( record -> record.get( 0 ).asInt() ) );
        assertEquals( 20000, ints.size() );
    }

    @Test
    public void shouldPropagateFailureFromSummary()
    {
        StatementResultCursor cursor = await( session.runAsync( "RETURN Something" ) );

        try
        {
            await( cursor.summaryAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.code(), containsString( "SyntaxError" ) );
        }

        assertNotNull( await( cursor.summaryAsync() ) );
    }

    @Test
    public void shouldPropagateFailureInCloseFromPreviousRun()
    {
        session.runAsync( "CREATE ()" );
        session.runAsync( "CREATE ()" );
        session.runAsync( "CREATE ()" );
        session.runAsync( "RETURN invalid" );

        try
        {
            await( session.closeAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.code(), containsString( "SyntaxError" ) );
        }
    }

    @Test
    public void shouldCloseCleanlyAfterFailure()
    {
        CompletionStage<StatementResultCursor> runWithOpenTx = session.beginTransactionAsync()
                .thenCompose( tx -> session.runAsync( "RETURN 1" ) );

        try
        {
            await( runWithOpenTx );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(),
                    startsWith( "Statements cannot be run directly on a session with an open transaction" ) );
        }

        await( session.closeAsync() );
    }

    @Test
    public void shouldPropagateFailureFromFirstIllegalQuery()
    {
        CompletionStage<StatementResultCursor> allStatements = session.runAsync( "CREATE (:Node1)" )
                .thenCompose( ignore -> session.runAsync( "CREATE (:Node2)" ) )
                .thenCompose( ignore -> session.runAsync( "RETURN invalid" ) )
                .thenCompose( ignore -> session.runAsync( "CREATE (:Node3)" ) );

        try
        {
            await( allStatements );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e, is( syntaxError( "Variable `invalid` not defined" ) ) );
        }

        assertEquals( 1, countNodesByLabel( "Node1" ) );
        assertEquals( 1, countNodesByLabel( "Node2" ) );
        assertEquals( 0, countNodesByLabel( "Node3" ) );
    }

    @Test
    public void shouldBePossibleToMixRunAsyncAndBlockingSessionClose()
    {
        long nodeCount = 5_000;

        try ( Session session = neo4j.driver().session() )
        {
            session.runAsync( "UNWIND range(1, " + nodeCount + ") AS x CREATE (n:AsyncNode {x: x}) RETURN n" );
        }

        assertEquals( nodeCount, countNodesByLabel( "AsyncNode" ) );
    }

    @Test
    public void shouldFailToExecuteBlockingRunInAsyncTransactionFunction()
    {
        TransactionWork<CompletionStage<List<Record>>> completionStageTransactionWork = tx ->
        {
            StatementResult result = tx.run( "UNWIND range(1, 10000) AS x CREATE (n:AsyncNode {x: x}) RETURN n" );
            List<Record> records = new ArrayList<>();
            while ( result.hasNext() )
            {
                records.add( result.next() );
            }

            return completedFuture( records );
        };

        CompletionStage<List<Record>> result = session.readTransactionAsync( completionStageTransactionWork );

        try
        {
            await( result );
            fail( "Exception expected" );
        }
        catch ( IllegalStateException e )
        {
            assertThat( e, is( blockingOperationInEventLoopError() ) );
        }
    }

    @Test
    public void shouldFailToExecuteBlockingRunChainedWithAsyncRun()
    {
        CompletionStage<StatementResult> result = session.runAsync( "RETURN 1" )
                .thenCompose( StatementResultCursor::singleAsync )
                .thenApply( record -> session.run( "RETURN $x", parameters( "x", record.get( 0 ).asInt() ) ) );

        try
        {
            await( result );
            fail( "Exception expected" );
        }
        catch ( IllegalStateException e )
        {
            assertThat( e, is( blockingOperationInEventLoopError() ) );
        }
    }

    @Test
    public void shouldAllowBlockingOperationInCommonPoolWhenChaining()
    {
        CompletionStage<Node> nodeStage = session.runAsync( "RETURN 42 AS value" )
                .thenCompose( StatementResultCursor::singleAsync )
                // move execution to ForkJoinPool.commonPool()
                .thenApplyAsync( record -> session.run( "CREATE (n:Node {value: $value}) RETURN n", record ) )
                .thenApply( StatementResult::single )
                .thenApply( record -> record.get( 0 ).asNode() );

        Node node = await( nodeStage );

        assertEquals( 42, node.get( "value" ).asInt() );
        assertEquals( 1, countNodesByLabel( "Node" ) );
    }

    @Test
    public void shouldAllowAccessingRecordsAfterSummary()
    {
        int recordCount = 10_000;
        String query = "UNWIND range(1, " + recordCount + ") AS x RETURN 'Hello-' + x";

        CompletionStage<SummaryAndRecords> summaryAndRecordsStage = session.runAsync( query )
                .thenCompose( cursor -> cursor.summaryAsync().thenCompose( summary -> cursor.listAsync()
                        .thenApply( records -> new SummaryAndRecords( summary, records ) ) ) );

        SummaryAndRecords summaryAndRecords = await( summaryAndRecordsStage );
        ResultSummary summary = summaryAndRecords.summary;
        List<Record> records = summaryAndRecords.records;

        assertNotNull( summary );
        assertNotNull( records );

        assertEquals( neo4j.address().toString(), summary.server().address() );
        assertEquals( query, summary.statement().text() );
        assertEquals( StatementType.READ_ONLY, summary.statementType() );

        assertEquals( recordCount, records.size() );
        for ( int i = 1; i <= recordCount; i++ )
        {
            Record record = records.get( i - 1 );
            assertEquals( "Hello-" + i, record.get( 0 ).asString() );
        }
    }

    @Test
    public void shouldAllowAccessingRecordsAfterSessionClosed()
    {
        int recordCount = 7_500;
        String query = "UNWIND range(1, " + recordCount + ") AS x RETURN x";

        CompletionStage<List<Record>> recordsStage = session.runAsync( query )
                .thenCompose( cursor -> session.closeAsync().thenApply( ignore -> cursor ) )
                .thenCompose( StatementResultCursor::listAsync );

        List<Record> records = await( recordsStage );

        assertEquals( recordCount, records.size() );
        for ( int i = 1; i <= recordCount; i++ )
        {
            Record record = records.get( i - 1 );
            assertEquals( i, record.get( 0 ).asInt() );
        }
    }

    private Future<List<CompletionStage<Record>>> runNestedQueries( StatementResultCursor inputCursor )
    {
        CompletableFuture<List<CompletionStage<Record>>> resultFuture = new CompletableFuture<>();
        runNestedQueries( inputCursor, new ArrayList<>(), resultFuture );
        return resultFuture;
    }

    private void runNestedQueries( StatementResultCursor inputCursor, List<CompletionStage<Record>> stages,
            CompletableFuture<List<CompletionStage<Record>>> resultFuture )
    {
        final CompletionStage<Record> recordResponse = inputCursor.nextAsync();
        stages.add( recordResponse );

        recordResponse.whenComplete( ( record, error ) ->
        {
            if ( error != null )
            {
                resultFuture.completeExceptionally( error );
            }
            else if ( record != null )
            {
                runNestedQuery( inputCursor, record, stages, resultFuture );
            }
            else
            {
                resultFuture.complete( stages );
            }
        } );
    }

    private void runNestedQuery( StatementResultCursor inputCursor, Record record,
            List<CompletionStage<Record>> stages, CompletableFuture<List<CompletionStage<Record>>> resultFuture )
    {
        Node node = record.get( 0 ).asNode();
        long id = node.get( "id" ).asLong();
        long age = id * 10;

        CompletionStage<StatementResultCursor> response =
                session.runAsync( "MATCH (p:Person {id: $id}) SET p.age = $age RETURN p",
                parameters( "id", id, "age", age ) );

        response.whenComplete( ( cursor, error ) ->
        {
            if ( error != null )
            {
                resultFuture.completeExceptionally( Futures.completionErrorCause( error ) );
            }
            else
            {
                stages.add( cursor.nextAsync() );
                runNestedQueries( inputCursor, stages, resultFuture );
            }
        } );
    }

    private long countNodesByLabel( String label )
    {
        CompletionStage<Long> countStage = session.runAsync( "MATCH (n:" + label + ") RETURN count(n)" )
                .thenCompose( StatementResultCursor::singleAsync )
                .thenApply( record -> record.get( 0 ).asLong() );

        return await( countStage );
    }

    private void testForEach( String query, int expectedSeenRecords )
    {
        StatementResultCursor cursor = await( session.runAsync( query ) );

        AtomicInteger recordsSeen = new AtomicInteger();
        CompletionStage<ResultSummary> forEachDone = cursor.forEachAsync( record -> recordsSeen.incrementAndGet() );
        ResultSummary summary = await( forEachDone );

        assertNotNull( summary );
        assertEquals( query, summary.statement().text() );
        assertEquals( emptyMap(), summary.statement().parameters().asMap() );
        assertEquals( expectedSeenRecords, recordsSeen.get() );
    }

    private <T> void testList( String query, List<T> expectedList )
    {
        StatementResultCursor cursor = await( session.runAsync( query ) );
        List<Record> records = await( cursor.listAsync() );
        List<Object> actualList = new ArrayList<>();
        for ( Record record : records )
        {
            actualList.add( record.get( 0 ).asObject() );
        }
        assertEquals( expectedList, actualList );
    }

    private void testConsume( String query )
    {
        StatementResultCursor cursor = await( session.runAsync( query ) );
        ResultSummary summary = await( cursor.consumeAsync() );

        assertNotNull( summary );
        assertEquals( query, summary.statement().text() );
        assertEquals( emptyMap(), summary.statement().parameters().asMap() );

        // no records should be available, they should all be consumed
        assertNull( await( cursor.nextAsync() ) );
    }

    private static class InvocationTrackingWork implements TransactionWork<CompletionStage<Record>>
    {
        final String query;
        final AtomicInteger invocationCount;

        Iterator<RuntimeException> asyncFailures = emptyIterator();
        Iterator<RuntimeException> syncFailures = emptyIterator();

        InvocationTrackingWork( String query )
        {
            this.query = query;
            this.invocationCount = new AtomicInteger();
        }

        InvocationTrackingWork withAsyncFailures( RuntimeException... failures )
        {
            asyncFailures = Arrays.asList( failures ).iterator();
            return this;
        }

        InvocationTrackingWork withSyncFailures( RuntimeException... failures )
        {
            syncFailures = Arrays.asList( failures ).iterator();
            return this;
        }

        int invocationCount()
        {
            return invocationCount.get();
        }

        @Override
        public CompletionStage<Record> execute( Transaction tx )
        {
            invocationCount.incrementAndGet();

            if ( syncFailures.hasNext() )
            {
                throw syncFailures.next();
            }

            CompletableFuture<Record> resultFuture = new CompletableFuture<>();

            tx.runAsync( query ).whenComplete( ( cursor, error ) ->
                    processQueryResult( cursor, Futures.completionErrorCause( error ), resultFuture ) );

            return resultFuture;
        }

        private void processQueryResult( StatementResultCursor cursor, Throwable error,
                CompletableFuture<Record> resultFuture )
        {
            if ( error != null )
            {
                resultFuture.completeExceptionally( error );
                return;
            }

            cursor.nextAsync().whenComplete( ( record, fetchError ) ->
                    processFetchResult( record, Futures.completionErrorCause( fetchError ), resultFuture ) );
        }

        private void processFetchResult( Record record, Throwable error, CompletableFuture<Record> resultFuture )
        {
            if ( error != null )
            {
                resultFuture.completeExceptionally( error );
                return;
            }

            if ( record == null )
            {
                resultFuture.completeExceptionally( new AssertionError( "Record not available" ) );
                return;
            }

            if ( asyncFailures.hasNext() )
            {
                resultFuture.completeExceptionally( asyncFailures.next() );
            }
            else
            {
                resultFuture.complete( record );
            }
        }
    }

    private static class SummaryAndRecords
    {
        final ResultSummary summary;
        final List<Record> records;

        SummaryAndRecords( ResultSummary summary, List<Record> records )
        {
            this.summary = summary;
            this.records = records;
        }
    }
}
