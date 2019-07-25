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
package org.neo4j.driver.integration.reactive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.neo4j.driver.Record;
import org.neo4j.driver.Statement;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.Bookmark;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.RxStatementResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.StatementType;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.internal.InternalBookmark.parse;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.internal.util.Matchers.containsResultAvailableAfterAndResultConsumedAfter;
import static org.neo4j.driver.internal.util.Matchers.syntaxError;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;
import static org.neo4j.driver.util.TestUtil.await;

@EnabledOnNeo4jWith( BOLT_V4 )
@ParallelizableIT
class RxTransactionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private RxSession session;

    @BeforeEach
    void setUp()
    {
        session = neo4j.driver().rxSession();
    }

    @Test
    void shouldBePossibleToCommitEmptyTx()
    {
        Bookmark bookmarkBefore = session.lastBookmark();

        Mono<Void> commit = Mono.from( session.beginTransaction() ).flatMap( tx -> Mono.from( tx.commit() ) );
        StepVerifier.create( commit ).verifyComplete();

        Bookmark bookmarkAfter = session.lastBookmark();

        assertNotNull( bookmarkAfter );
        assertNotEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldBePossibleToRollbackEmptyTx()
    {
        Bookmark bookmarkBefore = session.lastBookmark();

        Mono<Void> rollback = Mono.from( session.beginTransaction() ).flatMap( tx -> Mono.from( tx.rollback() ) );
        StepVerifier.create( rollback ).verifyComplete();

        Bookmark bookmarkAfter = session.lastBookmark();
        assertEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldBePossibleToRunSingleStatementAndCommit()
    {
        Flux<Integer> ids = Flux.usingWhen( session.beginTransaction(),
                tx -> Flux.from( tx.run( "CREATE (n:Node {id: 42}) RETURN n" ).records() )
                        .map( record -> record.get( 0 ).asNode().get( "id" ).asInt() ),
                RxTransaction::commit, RxTransaction::rollback );

        StepVerifier.create( ids ).expectNext( 42 ).verifyComplete();
        assertEquals( 1, countNodes( 42 ) );
    }

    @Test
    void shouldBePossibleToRunSingleStatementAndRollback()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanRunCreate( tx );
        assertCanRollback( tx );


        assertEquals( 0, countNodes( 4242 ) );
    }

    @ParameterizedTest
    @MethodSource( "commit" )
    void shouldBePossibleToRunMultipleStatements( boolean commit )
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );

        RxStatementResult cursor1 = tx.run( "CREATE (n:Node {id: 1})" );
        await( cursor1.records() );

        RxStatementResult cursor2 = tx.run( "CREATE (n:Node {id: 2})" );
        await( cursor2.records() );

        RxStatementResult cursor3 = tx.run( "CREATE (n:Node {id: 1})" );
        await( cursor3.records() );

        assertCanCommitOrRollback( commit, tx );

        verifyCommittedOrRolledBack( commit );
    }

    @ParameterizedTest
    @MethodSource( "commit" )
    void shouldBePossibleToRunMultipleStatementsWithoutWaiting( boolean commit )
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );

        RxStatementResult cursor1 = tx.run( "CREATE (n:Node {id: 1})" );
        RxStatementResult cursor2 = tx.run( "CREATE (n:Node {id: 2})" );
        RxStatementResult cursor3 = tx.run( "CREATE (n:Node {id: 1})" );

        await( Flux.from( cursor1.records() ).concatWith( cursor2.records() ).concatWith( cursor3.records() ) );
        assertCanCommitOrRollback( commit, tx );

        verifyCommittedOrRolledBack( commit );
    }

    @ParameterizedTest
    @MethodSource( "commit" )
    void shouldBePossibleToRunMultipleStatementsWithoutStreaming( boolean commit )
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );

        RxStatementResult cursor1 = tx.run( "CREATE (n:Node {id: 1})" );
        RxStatementResult cursor2 = tx.run( "CREATE (n:Node {id: 2})" );
        RxStatementResult cursor3 = tx.run( "CREATE (n:Node {id: 1})" );

        await( Flux.from( cursor1.keys() ).concatWith( cursor2.keys() ).concatWith( cursor3.keys() ) );
        assertCanCommitOrRollback( commit, tx );

        verifyCommittedOrRolledBack( commit );
    }

    @Test
    void shouldFailToCommitAfterSingleWrongStatement()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertFailToRunWrongStatement( tx );
        assertThrows( ClientException.class, () -> await( tx.commit() ) );
    }

    @Test
    void shouldAllowRollbackAfterSingleWrongStatement()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertFailToRunWrongStatement( tx );
        assertCanRollback( tx );

    }

    @Test
    void shouldFailToCommitAfterCoupleCorrectAndSingleWrongStatement()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );

        assertCanRunCreate( tx );
        assertCanRunReturnOne( tx );
        assertFailToRunWrongStatement( tx );

        assertThrows( ClientException.class, () -> await( tx.commit() ) );
    }

    @Test
    void shouldAllowRollbackAfterCoupleCorrectAndSingleWrongStatement()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanRunCreate( tx );
        assertCanRunReturnOne( tx );
        assertFailToRunWrongStatement( tx );

        assertCanRollback( tx );
    }

    @Test
    void shouldNotAllowNewStatementsAfterAnIncorrectStatement()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertFailToRunWrongStatement( tx );

        RxStatementResult result = tx.run( "CREATE ()" );
        Exception e = assertThrows( Exception.class, () -> await( result.records() ) );
        assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );

        assertCanRollback( tx );
    }

    @Test
    void shouldFailBoBeginTxWithInvalidBookmark()
    {
        RxSession session = neo4j.driver().rxSession( builder().withBookmarks( parse( "InvalidBookmark" ) ).build() );

        ClientException e = assertThrows( ClientException.class, () -> await( session.beginTransaction() ) );
        assertThat( e.getMessage(), containsString( "InvalidBookmark" ) );
    }

    @Test
    void shouldBePossibleToCommitWhenCommitted()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanRunCreate( tx );
        assertCanCommit( tx );

        Mono<Void> secondCommit = Mono.from( tx.commit() );
        // second commit should wrap around a completed future
        StepVerifier.create( secondCommit ).verifyComplete();

    }

    @Test
    void shouldBePossibleToRollbackWhenRolledBack()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanRunCreate( tx );
        assertCanRollback( tx );

        Mono<Void> secondRollback = Mono.from( tx.rollback() );
        // second rollback should wrap around a completed future
        StepVerifier.create( secondRollback ).verifyComplete();
    }

    @Test
    void shouldFailToCommitWhenRolledBack()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanRunCreate( tx );
        assertCanRollback( tx );

        // should not be possible to commit after rollback
        ClientException e = assertThrows( ClientException.class, () -> await( tx.commit() ) );
        assertThat( e.getMessage(), containsString( "transaction has been rolled back" ) );
    }

    @Test
    void shouldFailToRollbackWhenCommitted()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanRunCreate( tx );
        assertCanCommit( tx );

        // should not be possible to rollback after commit
        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollback() ) );
        assertThat( e.getMessage(), containsString( "transaction has been committed" ) );

    }

    @Test
    void shouldAllowRollbackAfterFailedCommit()
    {
        Flux<Record> records = Flux.usingWhen( session.beginTransaction(),
                tx -> Flux.from( tx.run( "WRONG" ).records() ),
                RxTransaction::commit, RxTransaction::rollback );

        StepVerifier.create( records ).verifyErrorSatisfies( error ->
                assertThat( error.getMessage(), containsString( "Invalid input" ) ) );
    }

    @Test
    void shouldExposeStatementKeysForColumnsWithAliases()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "RETURN 1 AS one, 2 AS two, 3 AS three, 4 AS five" );

        List<String> keys = await( result.keys() );
        assertEquals( Arrays.asList( "one", "two", "three", "five" ), keys );

        assertCanRollback( tx ); // you still need to rollback the tx as tx will not automatically closed
    }

    @Test
    void shouldExposeStatementKeysForColumnsWithoutAliases()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "RETURN 1, 2, 3, 5" );

        List<String> keys = await( result.keys() );
        assertEquals( Arrays.asList( "1", "2", "3", "5" ), keys );

        assertCanRollback( tx ); // you still need to rollback the tx as tx will not automatically closed
    }

    @Test
    void shouldExposeResultSummaryForSimpleQuery()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        String query = "CREATE (p1:Person {name: $name1})-[:KNOWS]->(p2:Person {name: $name2}) RETURN p1, p2";
        Value params = parameters( "name1", "Bob", "name2", "John" );

        RxStatementResult result = tx.run( query, params );
        await( result.records() ); // we run and stream

        ResultSummary summary = await( Mono.from( result.summary() ) );

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

        assertCanRollback( tx ); // you still need to rollback the tx as tx will not automatically closed

    }

    @Test
    void shouldExposeResultSummaryForExplainQuery()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        String query = "EXPLAIN MATCH (n) RETURN n";

        RxStatementResult result = tx.run( query );
        await( result.records() ); // we run and stream

        ResultSummary summary = await( Mono.from( result.summary() ) );

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

        assertCanRollback( tx ); // you still need to rollback the tx as tx will not automatically closed
    }

    @Test
    void shouldExposeResultSummaryForProfileQuery()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        String query = "PROFILE MERGE (n {name: $name}) " +
                "ON CREATE SET n.created = timestamp() " +
                "ON MATCH SET n.counter = coalesce(n.counter, 0) + 1";

        Value params = parameters( "name", "Bob" );

        RxStatementResult result = tx.run( query, params );
        await( result.records() ); // we run and stream

        ResultSummary summary = await( Mono.from( result.summary() ) );

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

        assertCanRollback( tx ); // you still need to rollback the tx as tx will not automatically closed
    }

    @Test
    void shouldCancelRecordStream()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "UNWIND ['a', 'b', 'c'] AS x RETURN x" );

        Flux<String> abc = Flux.from( result.records() ).limitRate( 1 ).take( 1 ).map( record -> record.get( 0 ).asString() );
        StepVerifier.create( abc ).expectNext( "a" ).verifyComplete();

        assertCanRollback( tx ); // you still need to rollback the tx as tx will not automatically closed
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
        RuntimeException e = new RuntimeException();

        Flux<Record> records = Flux.usingWhen( session.beginTransaction(),
                tx -> Flux.from( tx.run( "RETURN 'Hi!'" ).records() ).doOnNext( record -> { throw e; } ),
                RxTransaction::commit,
                RxTransaction::rollback );

        StepVerifier.create( records ).expectErrorSatisfies( error -> assertEquals( e, error ) ).verify();
    }

    @Test
    void shouldConvertToListWithEmptyCursor()
    {
        testList( "CREATE (:Person)-[:KNOWS]->(:Person)", emptyList() );
    }

    @Test
    void shouldConvertToListWithNonEmptyCursor()
    {
        testList( "UNWIND [1, '1', 2, '2', 3, '3'] AS x RETURN x", Arrays.asList( 1L, "1", 2L, "2", 3L, "3" ) );
    }

    @Test
    void shouldConvertToTransformedListWithEmptyCursor()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "CREATE ()" );
        List<Map<String,Object>> maps = await( Flux.from( result.records() ).map( record -> record.get( 0 ).asMap() )  );
        assertEquals( 0, maps.size() );
        assertCanRollback( tx );
    }

    @Test
    void shouldConvertToTransformedListWithNonEmptyCursor()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "UNWIND ['a', 'b', 'c'] AS x RETURN x" );
        List<String> strings = await( Flux.from( result.records() ).map( record -> record.get( 0 ).asString() + "!" )  );

        assertEquals( Arrays.asList( "a!", "b!", "c!" ), strings );
        assertCanRollback( tx );
    }

    @Test
    void shouldFailWhenListTransformationFunctionFails()
    {
        RuntimeException e = new RuntimeException();

        Flux<Object> records = Flux.usingWhen( session.beginTransaction(),
                tx -> Flux.from( tx.run( "RETURN 'Hi!'" ).records() ).map( record -> { throw e; } ),
                RxTransaction::commit, RxTransaction::rollback );

        StepVerifier.create( records ).expectErrorSatisfies( error -> {
            assertEquals( e, error );
        } ).verify();
    }

    @Test
    void shouldFailToCommitWhenServerIsRestarted()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "RETURN 1" );

        assertThrows( ServiceUnavailableException.class, () -> {
            await( Flux.from( result.records() ).doOnSubscribe( subscription -> {
                neo4j.killDb();
            } ) );
            await( tx.commit() );
        } );

        assertCanRollback( tx );
    }

    @Test
    void shouldFailSingleWithEmptyCursor()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "MATCH (n:NoSuchLabel) RETURN n" );

        NoSuchElementException e = assertThrows( NoSuchElementException.class, () -> await( Flux.from( result.records() ).single() ) );
        assertThat( e.getMessage(), containsString( "Source was empty" ) );
        assertCanRollback( tx );
    }

    @Test
    void shouldFailSingleWithMultiRecordCursor()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "UNWIND ['a', 'b'] AS x RETURN x" );

        IndexOutOfBoundsException e = assertThrows( IndexOutOfBoundsException.class, () -> await( Flux.from( result.records() ).single() ) );
        assertThat( e.getMessage(), startsWith( "Source emitted more than one item" ) );
        assertCanRollback( tx );
    }

    @Test
    void shouldReturnSingleWithSingleRecordCursor()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "RETURN 'Hello!'" );

        Record record = await( Flux.from( result.records() ).single() );
        assertEquals( "Hello!", record.get( 0 ).asString() );
        assertCanRollback( tx );
    }

    @Test
    void shouldPropagateFailureFromFirstRecordInSingleAsync()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "UNWIND [0] AS x RETURN 10 / x" );

        ClientException e = assertThrows( ClientException.class, () -> await( Flux.from( result.records() ).single() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
        assertCanRollback( tx );
    }

    @Test
    void shouldPropagateFailureFromSecondRecordInSingleAsync()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "UNWIND [1, 0] AS x RETURN 10 / x" );

        ClientException e = assertThrows( ClientException.class, () -> await( Flux.from( result.records() ).single() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
        assertCanRollback( tx );
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
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanCommit( tx );

        CompletableFuture<Object> future = Mono.from( tx.commit() ).toFuture();
        assertTrue( future.isDone() );
    }

    @Test
    void shouldFailToCommitAfterRollback()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanRollback( tx );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commit() ) );
        assertEquals( "Can't commit, transaction has been rolled back", e.getMessage() );

    }

    @Test
    void shouldFailToCommitAfterTermination()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertFailToRunWrongStatement( tx );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commit() ) );
        assertThat( e.getMessage(), startsWith( "Transaction can't be committed" ) );
        assertCanRollback( tx );
    }

    @Test
    void shouldDoNothingWhenRolledBackSecondTime()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanRollback( tx );

        CompletableFuture<Object> future = Mono.from( tx.rollback() ).toFuture();
        assertTrue( future.isDone() );

    }

    @Test
    void shouldFailToRollbackAfterCommit()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertCanCommit( tx );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollback() ) );
        assertEquals( "Can't rollback, transaction has been committed", e.getMessage() );
    }

    @Test
    void shouldRollbackAfterTermination()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertFailToRunWrongStatement( tx );
        assertCanRollback( tx );
    }

    @ParameterizedTest
    @MethodSource( "commit" )
    void shouldFailToRunQueryAfterCommit( boolean commit )
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "CREATE (:MyLabel)" );
        await( result.records() );

        assertCanCommitOrRollback( commit, tx );

        Record record = await( Flux.from( session.run( "MATCH (n:MyLabel) RETURN count(n)" ).records() ).single() );
        if( commit )
        {
            assertEquals( 1, record.get( 0 ).asInt() );
        }
        else
        {
            assertEquals( 0, record.get( 0 ).asInt() );
        }

        ClientException e = assertThrows( ClientException.class, () -> await( tx.run( "CREATE (:MyOtherLabel)" ).records() ) );
        assertThat( e.getMessage(), containsString( "Cannot run more statements in this transaction, it has been " ) );
    }

    @Test
    void shouldFailToRunQueryWhenTerminated()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        assertFailToRunWrongStatement( tx );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.run( "CREATE (:MyOtherLabel)" ).records() ) );
        assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );

        assertCanRollback( tx );
    }

    @Test
    void shouldUpdateSessionBookmarkAfterCommit()
    {
        Bookmark bookmarkBefore = session.lastBookmark();

        await( Flux.usingWhen( session.beginTransaction(),
                tx -> tx.run( "CREATE (:MyNode)" ).records(),
                RxTransaction::commit,
                RxTransaction::rollback ) );

        Bookmark bookmarkAfter = session.lastBookmark();

        assertNotNull( bookmarkAfter );
        assertNotEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldFailToCommitWhenQueriesFailAndErrorNotConsumed() throws InterruptedException
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );

        RxStatementResult result1 = tx.run( "CREATE (:TestNode)" );
        RxStatementResult result2 = tx.run( "CREATE (:TestNode)" );
        RxStatementResult result3 = tx.run( "RETURN 10 / 0" );
        RxStatementResult result4 = tx.run( "CREATE (:TestNode)" );

        Flux<Record> records =
                Flux.from( result1.records() ).concatWith( result2.records() ).concatWith( result3.records() ).concatWith( result4.records() );
        ClientException e = assertThrows( ClientException.class, () -> await( records ) );
        assertEquals( "/ by zero", e.getMessage() );
        assertCanRollback( tx );
    }

    @Test
    void shouldNotRunUntilPublisherIsConnected() throws Throwable
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );

        RxStatementResult result1 = tx.run( "RETURN 1" );
        RxStatementResult result2 = tx.run( "RETURN 2" );
        RxStatementResult result3 = tx.run( "RETURN 3" );
        RxStatementResult result4 = tx.run( "RETURN 4" );

        Flux<Record> records =
                Flux.from( result4.records() ).concatWith( result3.records() ).concatWith( result2.records() ).concatWith( result1.records() );
        StepVerifier.create( records.map( record -> record.get( 0 ).asInt() ) )
                .expectNext( 4 )
                .expectNext( 3 )
                .expectNext( 2 )
                .expectNext( 1 )
                .verifyComplete();
        assertCanRollback( tx );
    }

    @ParameterizedTest
    @MethodSource( "commit" )
    void shouldNotPropagateRunFailureIfNotExecuted( boolean commit )
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );

        tx.run( "RETURN ILLEGAL" ); // no actually executed

        assertCanCommitOrRollback( commit, tx );
    }

    @Test
    void shouldPropagateRunFailureOnRecord()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "RETURN 42 / 0" );
        await( result.keys() ); // always returns keys

        ClientException e = assertThrows( ClientException.class, () -> await( result.records() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
        assertCanRollback( tx );
    }

    @Test
    void shouldFailToCommitWhenPullAllFailureIsConsumed()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxStatementResult result = tx.run( "FOREACH (value IN [1,2, 'aaa'] | CREATE (:Person {name: 10 / value}))" );

        ClientException e1 = assertThrows( ClientException.class, () -> await( result.records() ) );
        assertThat( e1.code(), containsString( "TypeError" ) );

        ClientException e2 = assertThrows( ClientException.class, () -> await( tx.commit() ) );
        assertThat( e2.getMessage(), startsWith( "Transaction can't be committed" ) );

        assertCanRollback( tx );
    }

    @Test
    void shouldNotPropagateRunFailureFromSummary()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );

        RxStatementResult result = tx.run( "RETURN Wrong" );
        ClientException e = assertThrows( ClientException.class, () -> await( result.records() ) );
        assertThat( e.code(), containsString( "SyntaxError" ) );

        await( result.summary() );
        assertCanRollback( tx );
    }

    @Test
    void shouldHandleNestedQueries() throws Throwable
    {
        int size = 12555;

        Flux<Integer> nodeIds = Flux.usingWhen( session.beginTransaction(),
                tx -> {
                    RxStatementResult result = tx.run( "UNWIND range(1, $size) AS x RETURN x", Collections.singletonMap( "size", size ) );
                    return Flux.from( result.records() ).limitRate( 20 ).flatMap( record -> {
                        int x = record.get( "x" ).asInt();
                        RxStatementResult innerResult = tx.run( "CREATE (n:Node {id: $x}) RETURN n.id", Collections.singletonMap( "x", x ) );
                        return innerResult.records();
                    } ).map( record -> record.get( 0 ).asInt() );
                },
                RxTransaction::commit, RxTransaction::rollback
        );

        StepVerifier.create( nodeIds ).expectNextCount( size ).verifyComplete();
    }

    private int countNodes( Object id )
    {
        RxStatementResult result = session.run( "MATCH (n:Node {id: $id}) RETURN count(n)", parameters( "id", id ) );
        return await( Flux.from( result.records() ).single().map( record -> record.get( 0 ).asInt() ) );
    }

    private void testForEach( String query, int expectedSeenRecords )
    {

        Flux<ResultSummary> summary = Flux.usingWhen( session.beginTransaction(), tx -> {
            RxStatementResult result = tx.run( query );
            AtomicInteger recordsSeen = new AtomicInteger();
            return Flux.from( result.records() )
                    .doOnNext( record -> recordsSeen.incrementAndGet() )
                    .then( Mono.from( result.summary() ) )
                    .doOnSuccess( s -> {
                        assertNotNull( s );
                        assertEquals( query, s.statement().text() );
                        assertEquals( emptyMap(), s.statement().parameters().asMap() );
                        assertEquals( expectedSeenRecords, recordsSeen.get() );
                    } );
            }, RxTransaction::commit, RxTransaction::rollback );

        StepVerifier.create( summary ).expectNextCount( 1 ).verifyComplete(); // we indeed get a summary.
    }

    private <T> void testList( String query, List<T> expectedList )
    {
        List<Object> actualList = new ArrayList<>();

        Flux<List<Record>> records = Flux.usingWhen( session.beginTransaction(),
                tx -> Flux.from( tx.run( query ).records() ).collectList(),
                RxTransaction::commit,
                RxTransaction::rollback );

        StepVerifier.create( records.single() ).consumeNextWith( allRecords -> {
            for ( Record record : allRecords )
            {
                actualList.add( record.get( 0 ).asObject() );
            }
        } ).verifyComplete();

        assertEquals( expectedList, actualList );
    }

    private void testConsume( String query )
    {
        Flux<ResultSummary> summary = Flux.usingWhen( session.beginTransaction(), tx ->
            tx.run( query ).summary(),
            RxTransaction::commit,
            RxTransaction::rollback
        );

        StepVerifier.create( summary.single() ).consumeNextWith( Assertions::assertNotNull ).verifyComplete();
    }

    private void verifyCommittedOrRolledBack( boolean commit )
    {
        if( commit )
        {
            assertEquals( 2, countNodes( 1 ) );
            assertEquals( 1, countNodes( 2 ) );
        }
        else
        {
            assertEquals( 0, countNodes( 1 ) );
            assertEquals( 0, countNodes( 2 ) );
        }
    }

    private void assertCanCommitOrRollback( boolean commit, RxTransaction tx )
    {
        if( commit )
        {
            assertCanCommit( tx );
        }
        else
        {
            assertCanRollback( tx );
        }
    }

    private void assertCanCommit( RxTransaction tx )
    {
        assertThat( await( tx.commit() ), equalTo( emptyList() ) );
    }

    private void assertCanRollback( RxTransaction tx )
    {
        assertThat( await( tx.rollback() ), equalTo( emptyList() ) );
    }

    private static Stream<Boolean> commit()
    {
        return Stream.of( true, false );
    }

    private static void assertCanRunCreate( RxTransaction tx )
    {
        RxStatementResult result = tx.run( "CREATE (n:Node {id: 4242}) RETURN n" );

        Record record = await( Flux.from(result.records()).single() );

        Node node = record.get( 0 ).asNode();
        assertEquals( "Node", single( node.labels() ) );
        assertEquals( 4242, node.get( "id" ).asInt() );
    }

    private static void assertFailToRunWrongStatement( RxTransaction tx )
    {
        RxStatementResult result = tx.run( "RETURN" );
        Exception e = assertThrows( Exception.class, () -> await( result.records() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
    }

    private void assertCanRunReturnOne( RxTransaction tx )
    {
        RxStatementResult result = tx.run( "RETURN 42" );
        List<Record> records = await( result.records() );
        assertThat( records.size(), equalTo( 1 ) );
        Record record = records.get( 0 );
        assertEquals( 42, record.get( 0 ).asInt() );
    }
}
