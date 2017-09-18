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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Response;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class TransactionAsyncIT
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
    public void shouldBePossibleToCommitEmptyTx()
    {
        String bookmarkBefore = session.lastBookmark();

        Transaction tx = await( session.beginTransactionAsync() );
        assertThat( await( tx.commitAsync() ), is( nullValue() ) );

        String bookmarkAfter = session.lastBookmark();

        assertNotNull( bookmarkAfter );
        assertNotEquals( bookmarkBefore, bookmarkAfter );
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
        assertThat( await( cursor.fetchAsync() ), is( true ) );
        Node node = cursor.current().get( 0 ).asNode();
        assertEquals( "Node", single( node.labels() ) );
        assertEquals( 42, node.get( "id" ).asInt() );

        assertNull( await( tx.commitAsync() ) );
        assertEquals( 1, countNodes( 42 ) );
    }

    @Test
    public void shouldBePossibleToRunSingleStatementAndRollback()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor = await( tx.runAsync( "CREATE (n:Node {id: 4242}) RETURN n" ) );
        assertThat( await( cursor.fetchAsync() ), is( true ) );
        Node node = cursor.current().get( 0 ).asNode();
        assertEquals( "Node", single( node.labels() ) );
        assertEquals( 4242, node.get( "id" ).asInt() );

        assertNull( await( tx.rollbackAsync() ) );
        assertEquals( 0, countNodes( 4242 ) );
    }

    @Test
    public void shouldBePossibleToRunMultipleStatementsAndCommit()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "CREATE (n:Node {id: 1})" ) );
        assertThat( await( cursor1.fetchAsync() ), is( false ) );

        StatementResultCursor cursor2 = await( tx.runAsync( "CREATE (n:Node {id: 2})" ) );
        assertThat( await( cursor2.fetchAsync() ), is( false ) );

        StatementResultCursor cursor3 = await( tx.runAsync( "CREATE (n:Node {id: 2})" ) );
        assertThat( await( cursor3.fetchAsync() ), is( false ) );

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
        assertThat( await( cursor1.fetchAsync() ), is( false ) );

        StatementResultCursor cursor2 = await( tx.runAsync( "CREATE (n:Node {id: 42})" ) );
        assertThat( await( cursor2.fetchAsync() ), is( false ) );

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
            assertSyntaxError( e );
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
            assertSyntaxError( e );
        }

        assertThat( await( tx.rollbackAsync() ), is( nullValue() ) );
    }

    @Test
    public void shouldFailToCommitAfterCoupleCorrectAndSingleWrongStatement()
    {
        Transaction tx = await( session.beginTransactionAsync() );

        StatementResultCursor cursor1 = await( tx.runAsync( "CREATE (n:Node) RETURN n" ) );
        assertThat( await( cursor1.fetchAsync() ), is( true ) );
        assertTrue( cursor1.current().get( 0 ).asNode().hasLabel( "Node" ) );

        StatementResultCursor cursor2 = await( tx.runAsync( "RETURN 42" ) );
        assertThat( await( cursor2.fetchAsync() ), is( true ) );
        assertEquals( 42, cursor2.current().get( 0 ).asInt() );

        try
        {
            await( tx.runAsync( "RETURN" ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertSyntaxError( e );
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
        assertThat( await( cursor1.fetchAsync() ), is( true ) );
        assertEquals( 4242, cursor1.current().get( 0 ).asInt() );

        StatementResultCursor cursor2 = await( tx.runAsync( "CREATE (n:Node) DELETE n RETURN 42" ) );
        assertThat( await( cursor2.fetchAsync() ), is( true ) );
        assertEquals( 42, cursor2.current().get( 0 ).asInt() );

        try
        {
            await( tx.runAsync( "RETURN" ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertSyntaxError( e );
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
            assertSyntaxError( e );
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

        Response<Void> secondCommit = tx.commitAsync();
        // second commit should return a completed future
        assertTrue( secondCommit.isDone() );
        assertNull( await( secondCommit ) );
    }

    @Test
    public void shouldBePossibleToRollbackWhenRolledBack()
    {
        Transaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE ()" );
        assertNull( await( tx.rollbackAsync() ) );

        Response<Void> secondRollback = tx.rollbackAsync();
        // second rollback should return a completed future
        assertTrue( secondRollback.isDone() );
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
            assertThat( e.getMessage(), containsString( "transaction has already been rolled back" ) );
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
            assertThat( e.getMessage(), containsString( "transaction has already been committed" ) );
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
        assertThat( summary.resultAvailableAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
        assertThat( summary.resultConsumedAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
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
        assertThat( summary.resultAvailableAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
        assertThat( summary.resultConsumedAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
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
        System.out.println( profileAsString );
        assertThat( profileAsString, containsString( "DbHits" ) );
        assertThat( profileAsString, containsString( "PageCacheHits" ) );
        assertThat( profileAsString, containsString( "AllNodesScan" ) );
        assertEquals( 0, summary.notifications().size() );
        assertThat( summary.resultAvailableAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
        assertThat( summary.resultConsumedAfter( TimeUnit.MILLISECONDS ), greaterThanOrEqualTo( 0L ) );
    }

    private int countNodes( Object id )
    {
        StatementResult result = session.run( "MATCH (n:Node {id: $id}) RETURN count(n)", parameters( "id", id ) );
        return result.single().get( 0 ).asInt();
    }

    private static void assertSyntaxError( Exception e )
    {
        assertThat( e, instanceOf( ClientException.class ) );
        assertThat( ((ClientException) e).code(), containsString( "SyntaxError" ) );
        assertThat( e.getMessage(), startsWith( "Unexpected end of input" ) );
    }
}
