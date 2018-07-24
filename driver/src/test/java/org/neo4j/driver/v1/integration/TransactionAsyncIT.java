/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1.integration;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.async.EventLoopGroupFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.ChannelTrackingDriverFactory;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.util.DatabaseExtension;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.internal.util.Matchers.blockingOperationInEventLoopError;
import static org.neo4j.driver.internal.util.Matchers.containsResultAvailableAfterAndResultConsumedAfter;
import static org.neo4j.driver.internal.util.Matchers.syntaxError;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOOKMARKS;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.TestUtil.await;

class TransactionAsyncIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Session session;

    @BeforeEach
    void setUp()
    {
        session = neo4j.driver().session();
    }

    @AfterEach
    void tearDown()
    {
        session.closeAsync();
    }

    @Test
    void shouldBePossibleToCommitEmptyTx()
    {
        String bookmarkBefore = session.lastBookmark();

        Transaction tx = await( session.beginTransactionAsync() );
        assertThat( await( tx.commitAsync() ), is( nullValue() ) );

        String bookmarkAfter = session.lastBookmark();

        if ( BOOKMARKS.availableIn( neo4j.version() ) )
        {
            // bookmarks are only supported in 3.1.0+
            assertNotNull( bookmarkAfter );
            assertNotEquals( bookmarkBefore, bookmarkAfter );
        }
    }

    @Test
    void shouldBePossibleToRollbackEmptyTx()
    {
        String bookmarkBefore = session.lastBookmark();

        Transaction tx = await( session.beginTransactionAsync() );
        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );

        String bookmarkAfter = session.lastBookmark();
        assertEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldBePossibleToRunSingleStatementAndCommit()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "CREATE (n:Node {id: 42}) RETURN n" ) );

        Record record = await( cursor.nextAsync() );
        assertNotNull( record );
        Node node = record.get( 0 ).asNode();
        assertEquals( "Node", single( node.labels() ) );
        assertEquals( 42, node.get( "id" ).asInt() );
        assertNull( await( cursor.nextAsync() ) );

        assertNull( await( tx.commitAsync() ) );
        assertEquals( 1, countNodes( 42 ) );
    }

    @Test
    void shouldBePossibleToRunSingleStatementAndRollback()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "CREATE (n:Node {id: 4242}) RETURN n" ) );
        Record record = await( cursor.nextAsync() );
        assertNotNull( record );
        Node node = record.get( 0 ).asNode();
        assertEquals( "Node", single( node.labels() ) );
        assertEquals( 4242, node.get( "id" ).asInt() );
        assertNull( await( cursor.nextAsync() ) );

        assertNull( await( tx.rollbackAsync() ) );
        assertEquals( 0, countNodes( 4242 ) );
    }

    @Test
    void shouldBePossibleToRunMultipleStatementsAndCommit()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "CREATE (n:Node {id: 1})" ) );
        assertNull( await( cursor1.nextAsync() ) );

        StatementResultCursor cursor2 = await( tx.runAsync( "CREATE (n:Node {id: 2})" ) );
        assertNull( await( cursor2.nextAsync() ) );

        StatementResultCursor cursor3 = await( tx.runAsync( "CREATE (n:Node {id: 2})" ) );
        assertNull( await( cursor3.nextAsync() ) );

        assertNull( await( tx.commitAsync() ) );
        assertEquals( 1, countNodes( 1 ) );
        assertEquals( 2, countNodes( 2 ) );
    }

    @Test
    void shouldBePossibleToRunMultipleStatementsAndCommitWithoutWaiting()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "CREATE (n:Node {id: 1})" );
        tx.runAsync( "CREATE (n:Node {id: 2})" );
        tx.runAsync( "CREATE (n:Node {id: 1})" );

        assertNull( await( tx.commitAsync() ) );
        assertEquals( 1, countNodes( 2 ) );
        assertEquals( 2, countNodes( 1 ) );
    }

    @Test
    void shouldBePossibleToRunMultipleStatementsAndRollback()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "CREATE (n:Node {id: 1})" ) );
        assertNull( await( cursor1.nextAsync() ) );

        StatementResultCursor cursor2 = await( tx.runAsync( "CREATE (n:Node {id: 42})" ) );
        assertNull( await( cursor2.nextAsync() ) );

        assertNull( await( tx.rollbackAsync() ) );
        assertEquals( 0, countNodes( 1 ) );
        assertEquals( 0, countNodes( 42 ) );
    }

    @Test
    void shouldBePossibleToRunMultipleStatementsAndRollbackWithoutWaiting()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "CREATE (n:Node {id: 1})" );
        tx.runAsync( "CREATE (n:Node {id: 42})" );

        assertNull( await( tx.rollbackAsync() ) );
        assertEquals( 0, countNodes( 1 ) );
        assertEquals( 0, countNodes( 42 ) );
    }

    @Test
    void shouldFailToCommitAfterSingleWrongStatement()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "RETURN" ) );

        Exception e = assertThrows( Exception.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );

        assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
    }

    @Test
    void shouldAllowRollbackAfterSingleWrongStatement()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "RETURN" ) );

        Exception e = assertThrows( Exception.class, () -> await( cursor.nextAsync() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );
    }

    @Test
    void shouldFailToCommitAfterCoupleCorrectAndSingleWrongStatement()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "CREATE (n:Node) RETURN n" ) );
        Record record1 = await( cursor1.nextAsync() );
        assertNotNull( record1 );
        assertTrue( record1.get( 0 ).asNode().hasLabel( "Node" ) );

        StatementResultCursor cursor2 = await( tx.runAsync( "RETURN 42" ) );
        Record record2 = await( cursor2.nextAsync() );
        assertNotNull( record2 );
        assertEquals( 42, record2.get( 0 ).asInt() );

        StatementResultCursor cursor3 = await( tx.runAsync( "RETURN" ) );

        Exception e = assertThrows( Exception.class, () -> await( cursor3.consumeAsync() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );

        assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
    }

    @Test
    void shouldAllowRollbackAfterCoupleCorrectAndSingleWrongStatement()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "RETURN 4242" ) );
        Record record1 = await( cursor1.nextAsync() );
        assertNotNull( record1 );
        assertEquals( 4242, record1.get( 0 ).asInt() );

        StatementResultCursor cursor2 = await( tx.runAsync( "CREATE (n:Node) DELETE n RETURN 42" ) );
        Record record2 = await( cursor2.nextAsync() );
        assertNotNull( record2 );
        assertEquals( 42, record2.get( 0 ).asInt() );

        StatementResultCursor cursor3 = await( tx.runAsync( "RETURN" ) );

        Exception e = assertThrows( Exception.class, () -> await( cursor3.summaryAsync() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );
    }

    @Test
    void shouldNotAllowNewStatementsAfterAnIncorrectStatement()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "RETURN" ) );

        Exception e1 = assertThrows( Exception.class, () -> await( cursor.nextAsync() ) );
        assertThat( e1, is( syntaxError( "Unexpected end of input" ) ) );

        ClientException e2 = assertThrows( ClientException.class, () -> tx.runAsync( "CREATE ()" ) );
        assertThat( e2.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );
    }

    @Test
    @EnabledOnNeo4jWith( BOOKMARKS )
    void shouldFailBoBeginTxWithInvalidBookmark()
    {
        Session session = neo4j.driver().session( "InvalidBookmark" );

        ClientException e = assertThrows( ClientException.class, () -> await( session.beginTransactionAsync() ) );
        assertThat( e.getMessage(), containsString( "InvalidBookmark" ) );
    }

    @Test
    void shouldBePossibleToCommitWhenCommitted()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.commitAsync() ) );

        CompletionStage<Void> secondCommit = tx.commitAsync();
        // second commit should return a completed future
        assertTrue( secondCommit.toCompletableFuture().isDone() );
        assertNull( await( secondCommit ) );
    }

    @Test
    void shouldBePossibleToRollbackWhenRolledBack()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.rollbackAsync() ) );

        CompletionStage<Void> secondRollback = tx.rollbackAsync();
        // second rollback should return a completed future
        assertTrue( secondRollback.toCompletableFuture().isDone() );
        assertNull( await( secondRollback ) );
    }

    @Test
    void shouldFailToCommitWhenRolledBack()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.rollbackAsync() ) );

        // should not be possible to commit after rollback
        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.getMessage(), containsString( "transaction has been rolled back" ) );
    }

    @Test
    void shouldFailToRollbackWhenCommitted()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.commitAsync() ) );

        // should not be possible to rollback after commit
        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.getMessage(), containsString( "transaction has been committed" ) );
    }

    @Test
    void shouldExposeStatementKeysForColumnsWithAliases()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 1 AS one, 2 AS two, 3 AS three, 4 AS five" ) );

        assertEquals( Arrays.asList( "one", "two", "three", "five" ), cursor.keys() );
    }

    @Test
    void shouldExposeStatementKeysForColumnsWithoutAliases()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 1, 2, 3, 5" ) );

        assertEquals( Arrays.asList( "1", "2", "3", "5" ), cursor.keys() );
    }

    @Test
    void shouldExposeResultSummaryForSimpleQuery()
    {
        String query = "CREATE (p1:Person {name: $name1})-[:KNOWS]->(p2:Person {name: $name2}) RETURN p1, p2";
        Value params = parameters( "name1", "Bob", "name2", "John" );

        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query, params ) );
        ResultSummary summary = await( cursor.summaryAsync() );

        assertEquals( new Statement( query, params ), summary.statement() );
        assertEquals( 2, summary.counters().nodesCreated() );
        assertEquals( 2, summary.counters().labelsAdded() );
        assertEquals( 2, summary.counters().propertiesSet() );
        assertEquals( 1, summary.counters().relationshipsCreated() );
        assertEquals( StatementType.READ_WRITE, summary.statementType() );
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
        String query = "EXPLAIN MATCH (n) RETURN n";

        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query ) );
        ResultSummary summary = await( cursor.summaryAsync() );

        assertEquals( new Statement( query ), summary.statement() );
        assertEquals( 0, summary.counters().nodesCreated() );
        assertEquals( 0, summary.counters().propertiesSet() );
        assertEquals( StatementType.READ_ONLY, summary.statementType() );
        assertTrue( summary.hasPlan() );
        assertFalse( summary.hasProfile() );
        assertNotNull( summary.plan() );
        // asserting on plan is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        assertThat( summary.plan().toString().toLowerCase(), containsString( "scan" ) );
        assertNull( summary.profile() );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    void shouldExposeResultSummaryForProfileQuery()
    {
        String query = "PROFILE MERGE (n {name: $name}) " +
                       "ON CREATE SET n.created = timestamp() " +
                       "ON MATCH SET n.counter = coalesce(n.counter, 0) + 1";

        Value params = parameters( "name", "Bob" );

        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query, params ) );
        ResultSummary summary = await( cursor.summaryAsync() );

        assertEquals( new Statement( query, params ), summary.statement() );
        assertEquals( 1, summary.counters().nodesCreated() );
        assertEquals( 2, summary.counters().propertiesSet() );
        assertEquals( 0, summary.counters().relationshipsCreated() );
        assertEquals( StatementType.WRITE_ONLY, summary.statementType() );
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
    void shouldPeekRecordFromCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND ['a', 'b', 'c'] AS x RETURN x" ) );

        assertEquals( "a", await( cursor.peekAsync() ).get( 0 ).asString() );
        assertEquals( "a", await( cursor.peekAsync() ).get( 0 ).asString() );

        assertEquals( "a", await( cursor.nextAsync() ).get( 0 ).asString() );

        assertEquals( "b", await( cursor.peekAsync() ).get( 0 ).asString() );
        assertEquals( "b", await( cursor.peekAsync() ).get( 0 ).asString() );
        assertEquals( "b", await( cursor.peekAsync() ).get( 0 ).asString() );

        assertEquals( "b", await( cursor.nextAsync() ).get( 0 ).asString() );
        assertEquals( "c", await( cursor.nextAsync() ).get( 0 ).asString() );

        assertNull( await( cursor.peekAsync() ) );
        assertNull( await( cursor.nextAsync() ) );

        await( tx.rollbackAsync() );
    }

    @Test
    void shouldForEachWithEmptyCursor()
    {
        testForEach( "MATCH (n:SomeReallyStrangeLabel) RETURN n", 0 );
    }

    @Test
    void shouldForEachWithNonEmptyCursor()
    {
        testForEach( "UNWIND range(1, 12555) AS x CREATE (n:Node {id: x}) RETURN n", 12555 );
    }

    @Test
    void shouldFailForEachWhenActionFails()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 'Hi!'" ) );
        RuntimeException error = new RuntimeException();

        RuntimeException e = assertThrows( RuntimeException.class, () ->
        {
            await( cursor.forEachAsync( record ->
            {
                throw error;
            } ) );
        } );
        assertEquals( error, e );
    }

    @Test
    void shouldConvertToListWithEmptyCursor()
    {
        testList( "CREATE (:Person)-[:KNOWS]->(:Person)", Collections.emptyList() );
    }

    @Test
    void shouldConvertToListWithNonEmptyCursor()
    {
        testList( "UNWIND [1, '1', 2, '2', 3, '3'] AS x RETURN x", Arrays.asList( 1L, "1", 2L, "2", 3L, "3" ) );
    }

    @Test
    void shouldConvertToTransformedListWithEmptyCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "CREATE ()" ) );
        List<Map<String,Object>> maps = await( cursor.listAsync( record -> record.get( 0 ).asMap() ) );
        assertEquals( 0, maps.size() );
    }

    @Test
    void shouldConvertToTransformedListWithNonEmptyCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND ['a', 'b', 'c'] AS x RETURN x" ) );
        List<String> strings = await( cursor.listAsync( record -> record.get( 0 ).asString() + "!" ) );
        assertEquals( Arrays.asList( "a!", "b!", "c!" ), strings );
    }

    @Test
    void shouldFailWhenListTransformationFunctionFails()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 'Hello'" ) );
        IOException error = new IOException( "World" );

        Exception e = assertThrows( Exception.class, () ->
                await( cursor.listAsync( record ->
                {
                    throw new CompletionException( error );
                } ) ) );
        assertEquals( error, e );
    }

    @Test
    void shouldFailToCommitWhenServerIsRestarted()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "CREATE ()" ) );

        neo4j.killDb();

        assertThrows( ServiceUnavailableException.class, () -> await( tx.commitAsync() ) );
    }

    @Test
    void shouldFailSingleWithEmptyCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "MATCH (n:NoSuchLabel) RETURN n" ) );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "result is empty" ) );
    }

    @Test
    void shouldFailSingleWithMultiRecordCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND ['a', 'b'] AS x RETURN x" ) );

        NoSuchRecordException e = assertThrows( NoSuchRecordException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), startsWith( "Expected a result with a single record" ) );
    }

    @Test
    void shouldReturnSingleWithSingleRecordCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 'Hello!'" ) );

        Record record = await( cursor.singleAsync() );

        assertEquals( "Hello!", record.get( 0 ).asString() );
    }

    @Test
    void shouldPropagateFailureFromFirstRecordInSingleAsync()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND [0] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldNotPropagateFailureFromSecondRecordInSingleAsync()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND [1, 0] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.singleAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldConsumeEmptyCursor()
    {
        testConsume( "MATCH (n:NoSuchLabel) RETURN n" );
    }

    @Test
    void shouldConsumeNonEmptyCursor()
    {
        testConsume( "RETURN 42" );
    }

    @Test
    void shouldDoNothingWhenCommittedSecondTime()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        assertNull( await( tx.commitAsync() ) );

        assertTrue( tx.commitAsync().toCompletableFuture().isDone() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldFailToCommitAfterRollback()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        assertNull( await( tx.rollbackAsync() ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertEquals( "Can't commit, transaction has been rolled back", e.getMessage() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldFailToCommitAfterTermination()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        ((ExplicitTransaction) tx).markTerminated();

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.getMessage(), startsWith( "Transaction can't be committed" ) );
    }

    @Test
    void shouldDoNothingWhenRolledBackSecondTime()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        assertNull( await( tx.rollbackAsync() ) );

        assertTrue( tx.rollbackAsync().toCompletableFuture().isDone() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldFailToRollbackAfterCommit()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        assertNull( await( tx.commitAsync() ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertEquals( "Can't rollback, transaction has been committed", e.getMessage() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldRollbackAfterTermination()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        ((ExplicitTransaction) tx).markTerminated();

        assertNull( await( tx.rollbackAsync() ) );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldFailToRunQueryAfterCommit()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        assertNull( await( tx.commitAsync() ) );

        assertEquals( 1, session.run( "MATCH (n:MyLabel) RETURN count(n)" ).single().get( 0 ).asInt() );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.runAsync( "CREATE (:MyOtherLabel)" ) ) );
        assertEquals( "Cannot run more statements in this transaction, it has been committed", e.getMessage() );
    }

    @Test
    void shouldFailToRunQueryAfterRollback()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        assertNull( await( tx.rollbackAsync() ) );

        assertEquals( 0, session.run( "MATCH (n:MyLabel) RETURN count(n)" ).single().get( 0 ).asInt() );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.runAsync( "CREATE (:MyOtherLabel)" ) ) );
        assertEquals( "Cannot run more statements in this transaction, it has been rolled back", e.getMessage() );
    }

    @Test
    void shouldFailToRunQueryWhenMarkedForFailure()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        tx.failure();

        ClientException e = assertThrows( ClientException.class, () -> await( tx.runAsync( "CREATE (:MyOtherLabel)" ) ) );
        assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );
    }

    @Test
    void shouldFailToRunQueryWhenTerminated()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        ((ExplicitTransaction) tx).markTerminated();

        ClientException e = assertThrows( ClientException.class, () -> await( tx.runAsync( "CREATE (:MyOtherLabel)" ) ) );
        assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );
    }

    @Test
    void shouldAllowQueriesWhenMarkedForSuccess()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );

        tx.success();

        tx.runAsync( "CREATE (:MyLabel)" );
        assertNull( await( tx.commitAsync() ) );

        assertEquals( 2, session.run( "MATCH (n:MyLabel) RETURN count(n)" ).single().get( 0 ).asInt() );
    }

    @Test
    @EnabledOnNeo4jWith( BOOKMARKS )
    void shouldUpdateSessionBookmarkAfterCommit()
    {
        String bookmarkBefore = session.lastBookmark();

        await( session.beginTransactionAsync()
                .thenCompose( tx -> tx.runAsync( "CREATE (:MyNode)" )
                        .thenCompose( ignore -> tx.commitAsync() ) ) );

        String bookmarkAfter = session.lastBookmark();

        assertNotNull( bookmarkAfter );
        assertNotEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldFailToCommitWhenQueriesFailAndErrorNotConsumed() throws InterruptedException
    {
        Transaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "CREATE (:TestNode)" );
        tx.runAsync( "CREATE (:TestNode)" );
        tx.runAsync( "RETURN 10 / 0" );
        tx.runAsync( "CREATE (:TestNode)" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertEquals( "/ by zero", e.getMessage() );
    }

    @Test
    void shouldPropagateRunFailureFromCommit()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "RETURN ILLEGAL" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.getMessage(), containsString( "ILLEGAL" ) );
    }

    @Test
    void shouldPropagateBlockedRunFailureFromCommit()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "RETURN 42 / 0" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldPropagateRunFailureFromRollback()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "RETURN ILLEGAL" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.getMessage(), containsString( "ILLEGAL" ) );
    }

    @Test
    void shouldPropagateBlockedRunFailureFromRollback()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "RETURN 42 / 0" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
    }

    @Test
    void shouldPropagatePullAllFailureFromCommit()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.code(), containsString( "TypeError" ) );
    }

    @Test
    void shouldPropagateBlockedPullAllFailureFromCommit()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.code(), containsString( "TypeError" ) );
    }

    @Test
    void shouldPropagatePullAllFailureFromRollback()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x" );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.code(), containsString( "TypeError" ) );
    }

    @Test
    void shouldPropagateBlockedPullAllFailureFromRollback()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        await( tx.runAsync( "UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertThat( e.code(), containsString( "TypeError" ) );
    }

    @Test
    void shouldFailToCommitWhenRunFailureIsConsumed()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN Wrong" ) );

        ClientException e1 = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e1.code(), containsString( "SyntaxError" ) );

        ClientException e2 = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e2.getMessage(), startsWith( "Transaction can't be committed" ) );
    }

    @Test
    void shouldFailToCommitWhenPullAllFailureIsConsumed()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync(
                "FOREACH (value IN [1,2, 'aaa'] | CREATE (:Person {name: 10 / value}))" ) );

        ClientException e1 = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e1.code(), containsString( "TypeError" ) );

        ClientException e2 = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e2.getMessage(), startsWith( "Transaction can't be committed" ) );
    }

    @Test
    void shouldRollbackWhenRunFailureIsConsumed()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN Wrong" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e.code(), containsString( "SyntaxError" ) );
        assertNull( await( tx.rollbackAsync() ) );
    }

    @Test
    void shouldRollbackWhenPullAllFailureIsConsumed()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND [1, 0] AS x RETURN 5 / x" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.consumeAsync() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
        assertNull( await( tx.rollbackAsync() ) );
    }

    @Test
    void shouldPropagateFailureFromSummary()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "RETURN Wrong" ) );

        ClientException e = assertThrows( ClientException.class, () -> await( cursor.summaryAsync() ) );
        assertThat( e.code(), containsString( "SyntaxError" ) );
        assertNotNull( await( cursor.summaryAsync() ) );
    }

    @Test
    void shouldFailToExecuteBlockingRunChainedWithAsyncTransaction()
    {
        CompletionStage<Void> result = session.beginTransactionAsync()
                .thenApply( tx ->
                {
                    if ( EventLoopGroupFactory.isEventLoopThread( Thread.currentThread() ) )
                    {
                        IllegalStateException e = assertThrows( IllegalStateException.class, () -> tx.run( "CREATE ()" ) );
                        assertThat( e, is( blockingOperationInEventLoopError() ) );
                    }
                    return null;
                } );

        assertNull( await( result ) );
    }

    @Test
    void shouldAllowUsingBlockingApiInCommonPoolWhenChaining()
    {
        CompletionStage<Transaction> txStage = session.beginTransactionAsync()
                // move execution to ForkJoinPool.commonPool()
                .thenApplyAsync( tx ->
                {
                    tx.run( "UNWIND [1,1,2] AS x CREATE (:Node {id: x})" );
                    tx.run( "CREATE (:Node {id: 42})" );
                    tx.success();
                    tx.close();
                    return tx;
                } );

        Transaction tx = await( txStage );

        assertFalse( tx.isOpen() );
        assertEquals( 2, countNodes( 1 ) );
        assertEquals( 1, countNodes( 2 ) );
        assertEquals( 1, countNodes( 42 ) );
    }

    @Test
    void shouldBePossibleToRunMoreTransactionsAfterOneIsTerminated()
    {
        Transaction tx1 = await( session.beginTransactionAsync() );
        ((ExplicitTransaction) tx1).markTerminated();

        // commit should fail, make session forget about this transaction and release the connection to the pool
        ClientException e = assertThrows( ClientException.class, () -> await( tx1.commitAsync() ) );
        assertThat( e.getMessage(), startsWith( "Transaction can't be committed" ) );

        await( session.beginTransactionAsync()
                .thenCompose( tx -> tx.runAsync( "CREATE (:Node {id: 42})" )
                        .thenCompose( StatementResultCursor::consumeAsync )
                        .thenApply( ignore -> tx )
                ).thenCompose( Transaction::commitAsync ) );

        assertEquals( 1, countNodes( 42 ) );
    }

    @Test
    void shouldPropagateCommitFailureAfterFatalError()
    {
        testCommitAndRollbackFailurePropagation( true );
    }

    @Test
    void shouldPropagateRollbackFailureAfterFatalError()
    {
        testCommitAndRollbackFailurePropagation( false );
    }

    private int countNodes( Object id )
    {
        StatementResult result = session.run( "MATCH (n:Node {id: $id}) RETURN count(n)", parameters( "id", id ) );
        return result.single().get( 0 ).asInt();
    }

    private void testForEach( String query, int expectedSeenRecords )
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query ) );

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
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query ) );
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
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( query ) );
        ResultSummary summary = await( cursor.consumeAsync() );

        assertNotNull( summary );
        assertEquals( query, summary.statement().text() );
        assertEquals( emptyMap(), summary.statement().parameters().asMap() );

        // no records should be available, they should all be consumed
        assertNull( await( cursor.nextAsync() ) );
    }

    private void testCommitAndRollbackFailurePropagation( boolean commit )
    {
        ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );
        Config config = Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();

        try ( Driver driver = driverFactory.newInstance( neo4j.uri(), neo4j.authToken(), RoutingSettings.DEFAULT, RetrySettings.DEFAULT, config ) )
        {
            try ( Session session = driver.session() )
            {
                Transaction tx = session.beginTransaction();

                // run query but do not consume the result
                tx.run( "UNWIND range(0, 10000) AS x RETURN x + 1" );

                IOException ioError = new IOException( "Connection reset by peer" );
                for ( Channel channel : driverFactory.channels() )
                {
                    // make channel experience a fatal network error
                    // run in the event loop thread and wait for the whole operation to complete
                    Future<ChannelPipeline> future = channel.eventLoop().submit( () -> channel.pipeline().fireExceptionCaught( ioError ) );
                    await( future );
                }

                CompletionStage<Void> commitOrRollback = commit ? tx.commitAsync() : tx.rollbackAsync();

                // commit/rollback should fail and propagate the network error
                ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> await( commitOrRollback ) );
                assertEquals( ioError, e.getCause() );
            }
        }
    }
}
