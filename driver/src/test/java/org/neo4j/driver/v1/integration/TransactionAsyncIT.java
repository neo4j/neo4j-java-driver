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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.ExplicitTransaction;
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
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.internal.util.Futures.getBlocking;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.internal.util.Matchers.containsResultAvailableAfterAndResultConsumedAfter;
import static org.neo4j.driver.internal.util.Matchers.syntaxError;
import static org.neo4j.driver.internal.util.ServerVersion.v3_1_0;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class TransactionAsyncIT
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
    public void shouldBePossibleToCommitEmptyTx()
    {
        String bookmarkBefore = session.lastBookmark();

        Transaction tx = await( session.beginTransactionAsync() );
        assertThat( await( tx.commitAsync() ), is( nullValue() ) );

        String bookmarkAfter = session.lastBookmark();

        if ( neo4j.version().greaterThanOrEqual( v3_1_0 ) )
        {
            // bookmarks are only supported in 3.1.0+
            assertNotNull( bookmarkAfter );
            assertNotEquals( bookmarkBefore, bookmarkAfter );
        }
    }

    @Test
    public void shouldBePossibleToRollbackEmptyTx()
    {
        String bookmarkBefore = session.lastBookmark();

        Transaction tx = await( session.beginTransactionAsync() );
        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );

        String bookmarkAfter = session.lastBookmark();
        assertEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    public void shouldBePossibleToRunSingleStatementAndCommit()
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
    public void shouldBePossibleToRunSingleStatementAndRollback()
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
    public void shouldBePossibleToRunMultipleStatementsAndCommit()
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
    public void shouldBePossibleToRunMultipleStatementsAndCommitWithoutWaiting()
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
    public void shouldBePossibleToRunMultipleStatementsAndRollback()
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
    public void shouldBePossibleToRunMultipleStatementsAndRollbackWithoutWaiting()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        tx.runAsync( "CREATE (n:Node {id: 1})" );
        tx.runAsync( "CREATE (n:Node {id: 42})" );

        assertNull( await( tx.rollbackAsync() ) );
        assertEquals( 0, countNodes( 1 ) );
        assertEquals( 0, countNodes( 42 ) );
    }

    @Test
    public void shouldFailToCommitAfterSingleWrongStatement()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        try
        {
            await( tx.runAsync( "RETURN" ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        }

        try
        {
            await( tx.commitAsync() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }
    }

    @Test
    public void shouldAllowRollbackAfterSingleWrongStatement()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        try
        {
            await( tx.runAsync( "RETURN" ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        }

        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );
    }

    @Test
    public void shouldFailToCommitAfterCoupleCorrectAndSingleWrongStatement()
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

        try
        {
            await( tx.runAsync( "RETURN" ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        }

        try
        {
            await( tx.commitAsync() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }
    }

    @Test
    public void shouldAllowRollbackAfterCoupleCorrectAndSingleWrongStatement()
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

        try
        {
            await( tx.runAsync( "RETURN" ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        }

        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );
    }

    @Test
    public void shouldNotAllowNewStatementsAfterAnIncorrectStatement()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        try
        {
            await( tx.runAsync( "RETURN" ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
        }

        try
        {
            tx.runAsync( "CREATE ()" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );
        }
    }

    @Test
    public void shouldFailBoBeginTxWithInvalidBookmark()
    {
        assumeTrue( "Neo4j " + neo4j.version() + " does not support bookmarks",
                neo4j.version().greaterThanOrEqual( v3_1_0 ) );

        Session session = neo4j.driver().session( "InvalidBookmark" );

        try
        {
            await( session.beginTransactionAsync() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertThat( e.getMessage(), containsString( "InvalidBookmark" ) );
        }
    }

    @Test
    public void shouldBePossibleToCommitWhenCommitted()
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
    public void shouldBePossibleToRollbackWhenRolledBack()
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
    public void shouldFailToCommitWhenRolledBack()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.rollbackAsync() ) );

        try
        {
            // should not be possible to commit after rollback
            await( tx.commitAsync() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertThat( e.getMessage(), containsString( "transaction has been rolled back" ) );
        }
    }

    @Test
    public void shouldFailToRollbackWhenCommitted()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.commitAsync() ) );

        try
        {
            // should not be possible to rollback after commit
            await( tx.rollbackAsync() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
            assertThat( e.getMessage(), containsString( "transaction has been committed" ) );
        }
    }

    @Test
    public void shouldExposeStatementKeysForColumnsWithAliases()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 1 AS one, 2 AS two, 3 AS three, 4 AS five" ) );

        assertEquals( Arrays.asList( "one", "two", "three", "five" ), cursor.keys() );
    }

    @Test
    public void shouldExposeStatementKeysForColumnsWithoutAliases()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 1, 2, 3, 5" ) );

        assertEquals( Arrays.asList( "1", "2", "3", "5" ), cursor.keys() );
    }

    @Test
    public void shouldExposeResultSummaryForSimpleQuery()
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
    public void shouldExposeResultSummaryForExplainQuery()
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
        assertThat( summary.plan().toString(), containsString( "AllNodesScan" ) );
        assertNull( summary.profile() );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    public void shouldExposeResultSummaryForProfileQuery()
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
        String profileAsString = summary.profile().toString();
        assertThat( profileAsString, containsString( "DbHits" ) );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary, containsResultAvailableAfterAndResultConsumedAfter() );
    }

    @Test
    public void shouldPeekRecordFromCursor()
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
    public void shouldForEachWithEmptyCursor()
    {
        testForEach( "MATCH (n:SomeReallyStrangeLabel) RETURN n", 0 );
    }

    @Test
    public void shouldForEachWithNonEmptyCursor()
    {
        testForEach( "UNWIND range(1, 12555) AS x CREATE (n:Node {id: x}) RETURN n", 12555 );
    }

    @Test
    public void shouldFailForEachWhenActionFails()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 'Hi!'" ) );
        RuntimeException error = new RuntimeException();

        try
        {
            await( cursor.forEachAsync( record ->
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
    public void shouldConvertToListWithEmptyCursor()
    {
        testList( "CREATE (:Person)-[:KNOWS]->(:Person)", Collections.emptyList() );
    }

    @Test
    public void shouldConvertToListWithNonEmptyCursor()
    {
        testList( "UNWIND [1, '1', 2, '2', 3, '3'] AS x RETURN x", Arrays.asList( 1L, "1", 2L, "2", 3L, "3" ) );
    }

    @Test
    public void shouldConvertToTransformedListWithEmptyCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "CREATE ()" ) );
        List<Map<String,Object>> maps = await( cursor.listAsync( record -> record.get( 0 ).asMap() ) );
        assertEquals( 0, maps.size() );
    }

    @Test
    public void shouldConvertToTransformedListWithNonEmptyCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND ['a', 'b', 'c'] AS x RETURN x" ) );
        List<String> strings = await( cursor.listAsync( record -> record.get( 0 ).asString() + "!" ) );
        assertEquals( Arrays.asList( "a!", "b!", "c!" ), strings );
    }

    @Test
    public void shouldFailWhenListTransformationFunctionFails()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 'Hello'" ) );
        IOException error = new IOException( "World" );

        try
        {
            await( cursor.listAsync( record ->
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
    public void shouldFailWhenServerIsRestarted()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        neo4j.killDb();

        try
        {
            await( tx.runAsync( "CREATE ()" ) );
            await( tx.commitAsync() );
            fail( "Exception expected" );
        }
        catch ( Throwable t )
        {
            assertThat( t, instanceOf( ServiceUnavailableException.class ) );
        }
    }

    @Test
    public void shouldFailSingleWithEmptyCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "MATCH (n:NoSuchLabel) RETURN n" ) );

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
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND ['a', 'b'] AS x RETURN x" ) );

        try
        {
            await( cursor.singleAsync() );
            fail( "Exception expected" );
        }
        catch ( NoSuchRecordException e )
        {
            assertThat( e.getMessage(), startsWith( "Expected a cursor with a single record" ) );
        }
    }

    @Test
    public void shouldReturnSingleWithSingleRecordCursor()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "RETURN 'Hello!'" ) );

        Record record = await( cursor.singleAsync() );

        assertEquals( "Hello!", record.get( 0 ).asString() );
    }

    @Test
    public void shouldPropagateFailureFromFirstRecordInSingleAsync()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND [0] AS x RETURN 10 / x" ) );

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
        Transaction tx = await( session.beginTransactionAsync() );
        StatementResultCursor cursor = await( tx.runAsync( "UNWIND [1, 0] AS x RETURN 10 / x" ) );

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
        testConsume( "MATCH (n:NoSuchLabel) RETURN n" );
    }

    @Test
    public void shouldConsumeNonEmptyCursor()
    {
        testConsume( "RETURN 42" );
    }

    @Test
    public void shouldDoNothingWhenCommittedSecondTime()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );

        assertNull( getBlocking( tx.commitAsync() ) );

        assertTrue( tx.commitAsync().toCompletableFuture().isDone() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldFailToCommitAfterRollback()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );

        assertNull( getBlocking( tx.rollbackAsync() ) );

        try
        {
            getBlocking( tx.commitAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertEquals( "Can't commit, transaction has been rolled back", e.getMessage() );
        }
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldFailToCommitAfterTermination()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );

        ((ExplicitTransaction) tx).markTerminated();

        try
        {
            getBlocking( tx.commitAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertEquals( "Can't commit, transaction has been terminated by `Session#reset()`", e.getMessage() );
        }
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldDoNothingWhenRolledBackSecondTime()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );

        assertNull( getBlocking( tx.rollbackAsync() ) );

        assertTrue( tx.rollbackAsync().toCompletableFuture().isDone() );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldFailToRollbackAfterCommit()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );

        assertNull( getBlocking( tx.commitAsync() ) );

        try
        {
            getBlocking( tx.rollbackAsync() );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertEquals( "Can't rollback, transaction has been committed", e.getMessage() );
        }
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldRollbackAfterTermination()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );

        ((ExplicitTransaction) tx).markTerminated();

        assertNull( getBlocking( tx.rollbackAsync() ) );
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldFailToRunQueryAfterCommit()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        assertNull( getBlocking( tx.commitAsync() ) );

        assertEquals( 1, session.run( "MATCH (n:MyLabel) RETURN count(n)" ).single().get( 0 ).asInt() );

        try
        {
            getBlocking( tx.runAsync( "CREATE (:MyOtherLabel)" ) );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertEquals( "Cannot run more statements in this transaction, it has been committed", e.getMessage() );
        }
    }

    @Test
    public void shouldFailToRunQueryAfterRollback()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        assertNull( getBlocking( tx.rollbackAsync() ) );

        assertEquals( 0, session.run( "MATCH (n:MyLabel) RETURN count(n)" ).single().get( 0 ).asInt() );

        try
        {
            getBlocking( tx.runAsync( "CREATE (:MyOtherLabel)" ) );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertEquals( "Cannot run more statements in this transaction, it has been rolled back", e.getMessage() );
        }
    }

    @Test
    public void shouldFailToRunQueryWhenMarkedForFailure()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        tx.failure();

        try
        {
            getBlocking( tx.runAsync( "CREATE (:MyOtherLabel)" ) );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );
        }
    }

    @Test
    public void shouldFailToRunQueryWhenTerminated()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        ((ExplicitTransaction) tx).markTerminated();

        try
        {
            getBlocking( tx.runAsync( "CREATE (:MyOtherLabel)" ) );
            fail( "Exception expected" );
        }
        catch ( ClientException e )
        {
            assertEquals( "Cannot run more statements in this transaction, it has been terminated by `Session#reset()`",
                    e.getMessage() );
        }
    }

    @Test
    public void shouldAllowQueriesWhenMarkedForSuccess()
    {
        Transaction tx = getBlocking( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );

        tx.success();

        tx.runAsync( "CREATE (:MyLabel)" );
        assertNull( getBlocking( tx.commitAsync() ) );

        assertEquals( 2, session.run( "MATCH (n:MyLabel) RETURN count(n)" ).single().get( 0 ).asInt() );
    }

    @Test
    public void shouldUpdateSessionBookmarkAfterCommit()
    {
        String bookmarkBefore = session.lastBookmark();

        getBlocking( session.beginTransactionAsync()
                .thenCompose( tx -> tx.runAsync( "CREATE (:MyNode)" )
                        .thenCompose( ignore -> tx.commitAsync() ) ) );

        String bookmarkAfter = session.lastBookmark();

        assertNotNull( bookmarkAfter );
        assertNotEquals( bookmarkBefore, bookmarkAfter );
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
}
