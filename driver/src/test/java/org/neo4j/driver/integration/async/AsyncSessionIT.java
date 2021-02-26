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
package org.neo4j.driver.integration.async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

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

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.AsyncTransactionWork;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.internal.util.Matchers.arithmeticError;
import static org.neo4j.driver.internal.util.Matchers.containsResultAvailableAfterAndResultConsumedAfter;
import static org.neo4j.driver.internal.util.Matchers.syntaxError;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V3;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.awaitAll;

@ParallelizableIT
class AsyncSessionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private AsyncSession session;

    @BeforeEach
    void setUp()
    {
        session = neo4j.driver().asyncSession();
    }

    @AfterEach
    void tearDown()
    {
        session.closeAsync();
    }

    @Test
    void shouldRunQueryWithEmptyResult()
    {
        ResultCursor cursor = await( session.runAsync( "CREATE (:Person)" ) );

        assertNull( await( cursor.nextAsync() ) );
    }

    @Test
    void shouldRunQueryWithSingleResult()
    {
        ResultCursor cursor = await( session.runAsync( "CREATE (p:Person {name: 'Nick Fury'}) RETURN p" ) );

        Record record = await( cursor.nextAsync() );
        assertNotNull( record );
        Node node = record.get( 0 ).asNode();
        assertEquals( "Person", single( node.labels() ) );
        assertEquals( "Nick Fury", node.get( "name" ).asString() );

        assertNull( await( cursor.nextAsync() ) );
    }

    @Test
    void shouldRunQueryWithMultipleResults()
    {
        ResultCursor cursor = await( session.runAsync( "UNWIND [1,2,3] AS x RETURN x" ) );

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
    void shouldFailForIncorrectQuery()
    {
        ResultCursor cursor = await( session.runAsync( "RETURN" ) );

        Exception e = assertThrows( Exception.class, () -> await( cursor.nextAsync() ) );
        assertThat( e, is( syntaxError() ) );
    }

    @Test
    void shouldFailWhenQueryFailsAtRuntime()
    {
        ResultCursor cursor = await( session.runAsync( "CYPHER runtime=interpreted UNWIND [1, 2, 0] AS x RETURN 10 / x" ) );

        Record record1 = await( cursor.nextAsync() );
        assertNotNull( record1 );
        assertEquals( 10, record1.get( 0 ).asInt() );

        Record record2 = await( cursor.nextAsync() );
        assertNotNull( record2 );
        assertEquals( 5, record2.get( 0 ).asInt() );

        Exception e = assertThrows( Exception.class, () -> await( cursor.nextAsync() ) );
        assertThat( e, is( arithmeticError() ) );
    }

    @Test
    void shouldAllowNestedQueries()
    {
        ResultCursor cursor =
                await( session.runAsync( "UNWIND [1, 2, 3] AS x CREATE (p:Person {id: x}) RETURN p" ) );

        Future<List<CompletionStage<Record>>> queriesExecuted = runNestedQueries( cursor );
        List<CompletionStage<Record>> futures = await( queriesExecuted );

        List<Record> futureResults = awaitAll( futures );
        assertEquals( 7, futureResults.size() );

        ResultCursor personCursor = await( session.runAsync( "MATCH (p:Person) RETURN p ORDER BY p.id" ) );

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
    void shouldAllowMultipleAsyncRunsWithoutConsumingResults()
    {
        int queryCount = 13;
        List<CompletionStage<ResultCursor>> cursors = new ArrayList<>();
        for ( int i = 0; i < queryCount; i++ )
        {
            cursors.add( session.runAsync( "CREATE (:Person)" ) );
        }

        List<CompletionStage<Record>> records = new ArrayList<>();
        for ( ResultCursor cursor : awaitAll( cursors ) )
        {
            records.add( cursor.nextAsync() );
        }

        awaitAll( records );

        await( session.closeAsync() );
        session = neo4j.driver().asyncSession();

        ResultCursor cursor = await( session.runAsync( "MATCH (p:Person) RETURN count(p)" ) );
        Record record = await( cursor.nextAsync() );
        assertNotNull( record );
        assertEquals( queryCount, record.get( 0 ).asInt() );
    }

    @Test
    void shouldExposeQueryKeysForColumnsWithAliases()
    {
        ResultCursor cursor = await( session.runAsync( "RETURN 1 AS one, 2 AS two, 3 AS three, 4 AS five" ) );

        assertEquals( Arrays.asList( "one", "two", "three", "five" ), cursor.keys() );
    }

    @Test
    void shouldExposeQueryKeysForColumnsWithoutAliases()
    {
        ResultCursor cursor = await( session.runAsync( "RETURN 1, 2, 3, 5" ) );

        assertEquals( Arrays.asList( "1", "2", "3", "5" ), cursor.keys() );
    }

    @Test
    void shouldExposeResultSummaryForSimpleQuery()
    {
        String query = "CREATE (:Node {id: $id, name: $name})";
        Value params = parameters( "id", 1, "name", "TheNode" );

        ResultCursor cursor = await( session.runAsync( query, params ) );
        ResultSummary summary = await( cursor.consumeAsync() );

        assertEquals( new Query( query, params ), summary.query() );
        assertEquals( 1, summary.counters().nodesCreated() );
        assertEquals( 1, summary.counters().labelsAdded() );
        assertEquals( 2, summary.counters().propertiesSet() );
        assertEquals( 0, summary.counters().relationshipsCreated() );
        assertEquals( QueryType.WRITE_ONLY, summary.queryType() );
        assertFalse( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertNull( summary.plan() );
        assertNull( summary.profile() );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    void shouldExposeResultSummaryForExplainQuery()
    {
        String query = "EXPLAIN CREATE (),() WITH * MATCH (n)-->(m) CREATE (n)-[:HI {id: 'id'}]->(m) RETURN n, m";

        ResultCursor cursor = await( session.runAsync( query ) );
        ResultSummary summary = await( cursor.consumeAsync() );

        assertEquals( new Query( query ), summary.query() );
        assertEquals( 0, summary.counters().nodesCreated() );
        assertEquals( 0, summary.counters().propertiesSet() );
        assertEquals( 0, summary.counters().relationshipsCreated() );
        assertEquals( QueryType.READ_WRITE, summary.queryType() );
        assertTrue( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertNotNull( summary.plan() );
        // asserting on plan is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        String planAsString = summary.plan().toString().toLowerCase();
        assertThat( planAsString, containsString( "create" ) );
        assertThat( planAsString, containsString( "expand" ) );
        assertNull( summary.profile() );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    void shouldExposeResultSummaryForProfileQuery()
    {
        String query = "PROFILE CREATE (:Node)-[:KNOWS]->(:Node) WITH * MATCH (n) RETURN n";

        ResultCursor cursor = await( session.runAsync( query ) );
        ResultSummary summary = await( cursor.consumeAsync() );

        assertEquals( new Query( query ), summary.query() );
        assertEquals( 2, summary.counters().nodesCreated() );
        assertEquals( 0, summary.counters().propertiesSet() );
        assertEquals( 1, summary.counters().relationshipsCreated() );
        assertEquals( QueryType.READ_WRITE, summary.queryType() );
        assertTrue( summary.hasPlan() );
        assertTrue( summary.hasProfile() );
        assertNotNull( summary.plan() );
        assertNotNull( summary.profile() );
        // asserting on profile is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        String profileAsString = summary.profile().toString().toLowerCase();
        assertThat( profileAsString, containsString( "hits" ) );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    void shouldRunAsyncTransactionWithoutRetries()
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
    void shouldRunAsyncTransactionWithRetriesOnAsyncFailures()
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
    void shouldRunAsyncTransactionWithRetriesOnSyncFailures()
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
    void shouldRunAsyncTransactionThatCanNotBeRetried()
    {
        InvocationTrackingWork work = new InvocationTrackingWork( "UNWIND [10, 5, 0] AS x CREATE (:Hi) RETURN 10/x" );
        CompletionStage<Record> txStage = session.writeTransactionAsync( work );

        assertThrows( ClientException.class, () -> await( txStage ) );
        assertEquals( 1, work.invocationCount() );
        assertEquals( 0, countNodesByLabel( "Hi" ) );
    }

    @Test
    void shouldRunAsyncTransactionThatCanNotBeRetriedAfterATransientFailure()
    {
        // first throw TransientException directly from work, retry can happen afterwards
        // then return a future failed with DatabaseException, retry can't happen afterwards
        InvocationTrackingWork work = new InvocationTrackingWork( "CREATE (:Person) RETURN 1" )
                .withSyncFailures( new TransientException( "Oh!", "Deadlock!" ) )
                .withAsyncFailures( new DatabaseException( "Oh!", "OutOfMemory!" ) );
        CompletionStage<Record> txStage = session.writeTransactionAsync( work );

        DatabaseException e = assertThrows( DatabaseException.class, () -> await( txStage ) );

        assertEquals( 1, e.getSuppressed().length );
        assertThat( e.getSuppressed()[0], instanceOf( TransientException.class ) );
        assertEquals( 2, work.invocationCount() );
        assertEquals( 0, countNodesByLabel( "Person" ) );
    }

    @Test
    void shouldPeekRecordFromCursor()
    {
        ResultCursor cursor = await( session.runAsync( "UNWIND [1, 2, 42] AS x RETURN x" ) );

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
    void shouldForEachWithEmptyCursor()
    {
        testForEach( "CREATE ()", 0 );
    }

    @Test
    void shouldForEachWithNonEmptyCursor()
    {
        testForEach( "UNWIND range(1, 100000) AS x RETURN x", 100000 );
    }

    @Test
    void shouldFailForEachWhenActionFails()
    {
        ResultCursor cursor = await( session.runAsync( "RETURN 42" ) );
        IOException error = new IOException( "Hi" );

        IOException e = assertThrows( IOException.class, () ->
                await( cursor.forEachAsync( record ->
                                            {
                                                throw new CompletionException( error );
                                            } ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldConvertToListWithEmptyCursor()
    {
        testList( "MATCH (n:NoSuchLabel) RETURN n", Collections.emptyList() );
    }

    @Test
    void shouldConvertToListWithNonEmptyCursor()
    {
        testList( "UNWIND range(1, 100, 10) AS x RETURN x",
                  Arrays.asList( 1L, 11L, 21L, 31L, 41L, 51L, 61L, 71L, 81L, 91L ) );
    }

    @Test
    void shouldConvertToTransformedListWithEmptyCursor()
    {
        ResultCursor cursor = await( session.runAsync( "CREATE ()" ) );
        List<String> strings = await( cursor.listAsync( record -> "Hi!" ) );
        assertEquals( 0, strings.size() );
    }

    @Test
    void shouldConvertToTransformedListWithNonEmptyCursor()
    {
        ResultCursor cursor = await( session.runAsync( "UNWIND [1,2,3] AS x RETURN x" ) );
        List<Integer> ints = await( cursor.listAsync( record -> record.get( 0 ).asInt() + 1 ) );
        assertEquals( Arrays.asList( 2, 3, 4 ), ints );
    }

    @Test
    void shouldFailWhenListTransformationFunctionFails()
    {
        ResultCursor cursor = await( session.runAsync( "RETURN 42" ) );
        RuntimeException error = new RuntimeException( "Hi!" );

        RuntimeException e = assertThrows( RuntimeException.class, () ->
                await( cursor.listAsync( record ->
                                         {
                                             throw error;
                                         } ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldFailSingleWithEmptyCursor()
    {
        ResultCursor cursor = await( session.runAsync( "CREATE ()" ) );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "result is empty" ) );
    }

    @Test
    void shouldFailSingleWithMultiRecordCursor()
    {
        ResultCursor cursor = await( session.runAsync( "UNWIND [1, 2, 3] AS x RETURN x" ) );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), startsWith( "Expected a result with a single record" ) );
    }

    @Test
    void shouldReturnSingleWithSingleRecordCursor()
    {
        ResultCursor cursor = await( session.runAsync( "RETURN 42" ) );

        Record record = await( cursor.singleAsync() );

        assertEquals( 42, record.get( 0 ).asInt() );
    }

    @Test
    void shouldPropagateFailureFromFirstRecordInSingleAsync()
    {
        ResultCursor cursor = await( session.runAsync( "UNWIND [0] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldNotPropagateFailureFromSecondRecordInSingleAsync()
    {
        ResultCursor cursor = await( session.runAsync( "UNWIND [1, 0] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldConsumeEmptyCursor()
    {
        testConsume( "CREATE ()" );
    }

    @Test
    void shouldConsumeNonEmptyCursor()
    {
        testConsume( "UNWIND [42, 42] AS x RETURN x" );
    }

    @Test
    @DisabledOnNeo4jWith( BOLT_V3 )
    void shouldRunAfterBeginTxFailureOnBookmark()
    {
        Bookmark illegalBookmark = InternalBookmark.parse( "Illegal Bookmark" );
        session = neo4j.driver().asyncSession( builder().withBookmarks( illegalBookmark ).build() );

        assertThrows( ClientException.class, () -> await( session.beginTransactionAsync() ) );

        ResultCursor cursor = await( session.runAsync( "RETURN 'Hello!'" ) );
        Record record = await( cursor.singleAsync() );
        assertEquals( "Hello!", record.get( 0 ).asString() );
    }

    @Test
    void shouldNotBeginTxAfterBeginTxFailureOnBookmark()
    {
        Bookmark illegalBookmark = InternalBookmark.parse( "Illegal Bookmark" );
        session = neo4j.driver().asyncSession( builder().withBookmarks( illegalBookmark ).build() );
        assertThrows( ClientException.class, () -> await( session.beginTransactionAsync() ) );
        assertThrows( ClientException.class, () -> await( session.beginTransactionAsync() ) );
    }

    @Test
    @EnabledOnNeo4jWith( BOLT_V3 )
    void shouldNotRunAfterBeginTxFailureOnBookmark()
    {
        Bookmark illegalBookmark = InternalBookmark.parse( "Illegal Bookmark" );
        session = neo4j.driver().asyncSession( builder().withBookmarks( illegalBookmark ).build() );
        assertThrows( ClientException.class, () -> await( session.beginTransactionAsync() ) );
        ResultCursor cursor = await( session.runAsync( "RETURN 'Hello!'" ) );
        assertThrows( ClientException.class, () -> await( cursor.singleAsync() ) );
    }

    @Test
    void shouldExecuteReadTransactionUntilSuccessWhenWorkThrows()
    {
        int maxFailures = 1;

        CompletionStage<Integer> result = session.readTransactionAsync( new AsyncTransactionWork<CompletionStage<Integer>>()
        {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<Integer> execute( AsyncTransaction tx )
            {
                if ( failures.getAndIncrement() < maxFailures )
                {
                    throw new SessionExpiredException( "Oh!" );
                }
                return tx.runAsync( "UNWIND range(1, 10) AS x RETURN count(x)" )
                         .thenCompose( ResultCursor::singleAsync )
                         .thenApply( record -> record.get( 0 ).asInt() );
            }
        } );

        assertEquals( 10, await( result ).intValue() );
    }

    @Test
    void shouldExecuteWriteTransactionUntilSuccessWhenWorkThrows()
    {
        int maxFailures = 2;

        CompletionStage<Integer> result = session.writeTransactionAsync( new AsyncTransactionWork<CompletionStage<Integer>>()
        {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<Integer> execute( AsyncTransaction tx )
            {
                if ( failures.getAndIncrement() < maxFailures )
                {
                    throw new ServiceUnavailableException( "Oh!" );
                }
                return tx.runAsync( "CREATE (n1:TestNode), (n2:TestNode) RETURN 2" )
                         .thenCompose( ResultCursor::singleAsync )
                         .thenApply( record -> record.get( 0 ).asInt() );
            }
        } );

        assertEquals( 2, await( result ).intValue() );
        assertEquals( 2, countNodesByLabel( "TestNode" ) );
    }

    @Test
    void shouldExecuteReadTransactionUntilSuccessWhenWorkFails()
    {
        int maxFailures = 3;

        CompletionStage<Integer> result = session.readTransactionAsync( new AsyncTransactionWork<CompletionStage<Integer>>()
        {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<Integer> execute( AsyncTransaction tx )
            {
                return tx.runAsync( "RETURN 42" )
                         .thenCompose( ResultCursor::singleAsync )
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
    void shouldExecuteWriteTransactionUntilSuccessWhenWorkFails()
    {
        int maxFailures = 2;

        CompletionStage<String> result = session.writeTransactionAsync( new AsyncTransactionWork<CompletionStage<String>>()
        {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<String> execute( AsyncTransaction tx )
            {
                return tx.runAsync( "CREATE (:MyNode) RETURN 'Hello'" )
                         .thenCompose( ResultCursor::singleAsync )
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
    void shouldPropagateRunFailureWhenClosed()
    {
        session.runAsync( "RETURN 10 / 0" );

        ClientException e = assertThrows( ClientException.class, () -> await( session.closeAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldPropagateBlockedRunFailureWhenClosed()
    {
        await( session.runAsync( "RETURN 10 / 0" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( session.closeAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    @EnabledOnNeo4jWith( BOLT_V4 )
    void shouldNotPropagateFailureWhenStreamingIsCancelled()
    {
        session.runAsync( "UNWIND range(20000, 0, -1) AS x RETURN 10 / x" );

        await( session.closeAsync() );
    }

    @Test
    @EnabledOnNeo4jWith( BOLT_V4 )
    void shouldNotPropagateBlockedPullAllFailureWhenClosed()
    {
        await( session.runAsync( "UNWIND range(20000, 0, -1) AS x RETURN 10 / x" ) );

        await( session.closeAsync() );
    }

    @Test
    void shouldCloseCleanlyWhenRunErrorConsumed()
    {
        ResultCursor cursor = await( session.runAsync( "SomeWrongQuery" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e.getMessage(), startsWith( "Invalid input" ) );
        assertNull( await( session.closeAsync() ) );
    }

    @Test
    void shouldCloseCleanlyWhenPullAllErrorConsumed()
    {
        ResultCursor cursor = await( session.runAsync( "UNWIND range(10, 0, -1) AS x RETURN 1 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
        assertNull( await( session.closeAsync() ) );
    }

    @Test
    void shouldPropagateFailureFromSummary()
    {
        ResultCursor cursor = await( session.runAsync( "RETURN Something" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e.code(), containsString( "SyntaxError" ) );
        assertNotNull( await( cursor.consumeAsync() ) );
    }

    @Test
    void shouldPropagateFailureInCloseFromPreviousRun()
    {
        session.runAsync( "CREATE ()" );
        session.runAsync( "CREATE ()" );
        session.runAsync( "CREATE ()" );
        session.runAsync( "RETURN invalid" );

        ClientException e = assertThrows( ClientException.class, () -> await( session.closeAsync() ) );
        assertThat( e.code(), containsString( "SyntaxError" ) );
    }

    @Test
    void shouldCloseCleanlyAfterFailure()
    {
        CompletionStage<ResultCursor> runWithOpenTx = session.beginTransactionAsync()
                                                             .thenCompose( tx -> session.runAsync( "RETURN 1" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( runWithOpenTx ) );
        assertThat( e.getMessage(), startsWith( "Queries cannot be run directly on a session with an open transaction" ) );

        await( session.closeAsync() );
    }

    @Test
    void shouldPropagateFailureFromFirstIllegalQuery()
    {
        CompletionStage<ResultCursor> allQueries = session.runAsync( "CREATE (:Node1)" )
                                                          .thenCompose( ignore -> session.runAsync( "CREATE (:Node2)" ) )
                                                          .thenCompose( ignore -> session.runAsync( "RETURN invalid" ) )
                                                          .thenCompose( ignore -> session.runAsync( "CREATE (:Node3)" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( allQueries ) );
        assertThat( e, is( syntaxError( "Variable `invalid` not defined" ) ) );

        assertEquals( 1, countNodesByLabel( "Node1" ) );
        assertEquals( 1, countNodesByLabel( "Node2" ) );
        assertEquals( 0, countNodesByLabel( "Node3" ) );
    }

    @Test
    void shouldAllowReturningNullFromAsyncTransactionFunction()
    {
        CompletionStage<Object> readResult = session.readTransactionAsync( tx -> null );
        assertNull( await( readResult ) );

        CompletionStage<Object> writeResult = session.writeTransactionAsync( tx -> null );
        assertNull( await( writeResult ) );
    }

    private Future<List<CompletionStage<Record>>> runNestedQueries( ResultCursor inputCursor )
    {
        CompletableFuture<List<CompletionStage<Record>>> resultFuture = new CompletableFuture<>();
        runNestedQueries( inputCursor, new ArrayList<>(), resultFuture );
        return resultFuture;
    }

    private void runNestedQueries( ResultCursor inputCursor, List<CompletionStage<Record>> stages,
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

    private void runNestedQuery( ResultCursor inputCursor, Record record,
                                 List<CompletionStage<Record>> stages, CompletableFuture<List<CompletionStage<Record>>> resultFuture )
    {
        Node node = record.get( 0 ).asNode();
        long id = node.get( "id" ).asLong();
        long age = id * 10;

        CompletionStage<ResultCursor> response =
                session.runAsync( "MATCH (p:Person {id: $id}) SET p.age = $age RETURN p",
                                  parameters( "id", id, "age", age ) );

        response.whenComplete( ( cursor, error ) ->
                               {
                                   if ( error != null )
                                   {
                                       resultFuture.completeExceptionally( Futures.completionExceptionCause( error ) );
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
                                                  .thenCompose( ResultCursor::singleAsync )
                                                  .thenApply( record -> record.get( 0 ).asLong() );

        return await( countStage );
    }

    private void testForEach( String query, int expectedSeenRecords )
    {
        ResultCursor cursor = await( session.runAsync( query ) );

        AtomicInteger recordsSeen = new AtomicInteger();
        CompletionStage<ResultSummary> forEachDone = cursor.forEachAsync( record -> recordsSeen.incrementAndGet() );
        ResultSummary summary = await( forEachDone );

        assertNotNull( summary );
        assertEquals( query, summary.query().text() );
        assertEquals( emptyMap(), summary.query().parameters().asMap() );
        assertEquals( expectedSeenRecords, recordsSeen.get() );
    }

    private <T> void testList( String query, List<T> expectedList )
    {
        ResultCursor cursor = await( session.runAsync( query ) );
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
        ResultCursor cursor = await( session.runAsync( query ) );
        ResultSummary summary = await( cursor.consumeAsync() );

        assertNotNull( summary );
        assertEquals( query, summary.query().text() );
        assertEquals( emptyMap(), summary.query().parameters().asMap() );

        // no records should be available, they should all be consumed
        assertThrows( ResultConsumedException.class, () -> await( cursor.nextAsync() ) );
    }

    private static class InvocationTrackingWork implements AsyncTransactionWork<CompletionStage<Record>>
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
        public CompletionStage<Record> execute( AsyncTransaction tx )
        {
            invocationCount.incrementAndGet();

            if ( syncFailures.hasNext() )
            {
                throw syncFailures.next();
            }

            CompletableFuture<Record> resultFuture = new CompletableFuture<>();

            tx.runAsync( query ).whenComplete( ( cursor, error ) ->
                                                       processQueryResult( cursor, Futures.completionExceptionCause( error ), resultFuture ) );

            return resultFuture;
        }

        private void processQueryResult( ResultCursor cursor, Throwable error,
                                         CompletableFuture<Record> resultFuture )
        {
            if ( error != null )
            {
                resultFuture.completeExceptionally( error );
                return;
            }

            cursor.nextAsync().whenComplete( ( record, fetchError ) ->
                                                     processFetchResult( record, Futures.completionExceptionCause( fetchError ), resultFuture ) );
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
}
