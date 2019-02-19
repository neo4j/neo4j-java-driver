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
package org.neo4j.driver.v1.integration.reactive;

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

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.util.DatabaseExtension;
import org.neo4j.driver.v1.util.ParallelizableIT;

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
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.internal.util.Matchers.containsResultAvailableAfterAndResultConsumedAfter;
import static org.neo4j.driver.internal.util.Matchers.syntaxError;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOOKMARKS;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.TestUtil.await;

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
        String bookmarkBefore = session.lastBookmark();

        Mono<Void> commit = Mono.from( session.beginTransaction() ).flatMap( tx -> Mono.from( tx.commit() ) );
        StepVerifier.create( commit ).verifyComplete();

        String bookmarkAfter = session.lastBookmark();

        assertNotNull( bookmarkAfter );
        assertNotEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldBePossibleToRollbackEmptyTx()
    {
        String bookmarkBefore = session.lastBookmark();

        Mono<Void> rollback = Mono.from( session.beginTransaction() ).flatMap( tx -> Mono.from( tx.rollback() ) );
        StepVerifier.create( rollback ).verifyComplete();

        String bookmarkAfter = session.lastBookmark();
        assertEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldBePossibleToRunSingleStatementAndCommit()
    {

        Mono<RxTransaction> txRun = Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( "CREATE (n:Node {id: 42}) RETURN n" );
            Flux<Integer> nodeIds = Flux.from( result.records() )
                    .map( record -> record.get( 0 ).asNode().get( "id" ).asInt() )
                    .concatWith( tx.commit() )
                    .onErrorResume( error -> Mono.from( tx.rollback() ).then( Mono.error( error ) ) );
            StepVerifier.create( nodeIds ).expectNext( 42 ).verifyComplete();
        } );

        StepVerifier.create( txRun ).expectNextCount( 1 ).verifyComplete();
        assertEquals( 1, countNodes( 42 ) );
    }

    @Test
    void shouldBePossibleToRunSingleStatementAndRollback()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanRunCreate( tx );
            assertCanRollback( tx );
        } ) );

        assertEquals( 0, countNodes( 4242 ) );
    }

    @ParameterizedTest
    @MethodSource( "commit" )
    void shouldBePossibleToRunMultipleStatements( boolean commit )
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {

            RxResult cursor1 = tx.run( "CREATE (n:Node {id: 1})" );
            await( cursor1.records() );

            RxResult cursor2 = tx.run( "CREATE (n:Node {id: 2})" );
            await( cursor2.records() );

            RxResult cursor3 = tx.run( "CREATE (n:Node {id: 1})" );
            await( cursor3.records() );

            assertCanCommitOrRollback( commit, tx );
        } ) );

        verifyCommittedOrRolledBack( commit );
    }

    @ParameterizedTest
    @MethodSource( "commit" )
    void shouldBePossibleToRunMultipleStatementsWithoutWaiting( boolean commit )
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {

            RxResult cursor1 = tx.run( "CREATE (n:Node {id: 1})" );
            RxResult cursor2 = tx.run( "CREATE (n:Node {id: 2})" );
            RxResult cursor3 = tx.run( "CREATE (n:Node {id: 1})" );

            await( Flux.from( cursor1.records() ).concatWith( cursor2.records() ).concatWith( cursor3.records() ) );
            assertCanCommitOrRollback( commit, tx );
        } ) );

        verifyCommittedOrRolledBack( commit );
    }

    @ParameterizedTest
    @MethodSource( "commit" )
    void shouldBePossibleToRunMultipleStatementsWithoutStreaming( boolean commit )
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {

            RxResult cursor1 = tx.run( "CREATE (n:Node {id: 1})" );
            RxResult cursor2 = tx.run( "CREATE (n:Node {id: 2})" );
            RxResult cursor3 = tx.run( "CREATE (n:Node {id: 1})" );

            await( Flux.from( cursor1.keys() ).concatWith( cursor2.keys() ).concatWith( cursor3.keys() ) );
            assertCanCommitOrRollback( commit, tx );
        } ) );

        verifyCommittedOrRolledBack( commit );
    }

    @Test
    void shouldFailToCommitAfterSingleWrongStatement()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertFailToRunWrongStatement( tx );
            assertThrows( ClientException.class, () -> await( tx.commit() ) );
        } ) );
    }

    @Test
    void shouldAllowRollbackAfterSingleWrongStatement()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertFailToRunWrongStatement( tx );
            assertCanRollback( tx );
        } ) );
    }

    @Test
    void shouldFailToCommitAfterCoupleCorrectAndSingleWrongStatement()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanRunCreate( tx );
            assertCanRunReturnOne( tx );
            assertFailToRunWrongStatement( tx );

            assertThrows( ClientException.class, () -> await( tx.commit() ) );
        }));
    }

    @Test
    void shouldAllowRollbackAfterCoupleCorrectAndSingleWrongStatement()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanRunCreate( tx );
            assertCanRunReturnOne( tx );
            assertFailToRunWrongStatement( tx );

            assertCanRollback( tx );
        }));
    }

    @Test
    void shouldNotAllowNewStatementsAfterAnIncorrectStatement()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertFailToRunWrongStatement( tx );

            RxResult result = tx.run( "CREATE ()" );
            Exception e = assertThrows( Exception.class, () -> await( result.records() ) );
            assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );

            assertCanRollback( tx );
        }));
    }

    @Test
    @EnabledOnNeo4jWith( BOOKMARKS )
    void shouldFailBoBeginTxWithInvalidBookmark()
    {
        RxSession session = neo4j.driver().rxSession( "InvalidBookmark" );

        ClientException e = assertThrows( ClientException.class, () -> await( session.beginTransaction() ) );
        assertThat( e.getMessage(), containsString( "InvalidBookmark" ) );
    }

    @Test
    void shouldBePossibleToCommitWhenCommitted()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanRunCreate( tx );
            assertCanCommit( tx );

            Mono<Void> secondCommit = Mono.from( tx.commit() );
            // second commit should wrap around a completed future
            StepVerifier.create( secondCommit ).verifyComplete();
        }));
    }

    @Test
    void shouldBePossibleToRollbackWhenRolledBack()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanRunCreate( tx );
            assertCanRollback( tx );

            Mono<Void> secondRollback = Mono.from( tx.rollback() );
            // second rollback should wrap around a completed future
            StepVerifier.create( secondRollback ).verifyComplete();
        } ) );
    }

    @Test
    void shouldFailToCommitWhenRolledBack()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanRunCreate( tx );
            assertCanRollback( tx );

            // should not be possible to commit after rollback
            ClientException e = assertThrows( ClientException.class, () -> await( tx.commit() ) );
            assertThat( e.getMessage(), containsString( "transaction has been rolled back" ) );
        } ) );
    }

    @Test
    void shouldFailToRollbackWhenCommitted()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanRunCreate( tx );
            assertCanCommit( tx );

            // should not be possible to rollback after commit
            ClientException e = assertThrows( ClientException.class, () -> await( tx.rollback() ) );
            assertThat( e.getMessage(), containsString( "transaction has been committed" ) );
        } ) );
    }

    @Test
    void shouldAllowRollbackAfterFailedCommit()
    {
        Mono<RxTransaction> txRun = Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( "WRONG" );
            Flux<Record> records = Flux.from( result.records() )
                    .concatWith( tx.commit() ) // if we completed without error then we commit
                    .onErrorResume( error -> Mono.from( tx.rollback() ) // otherwise we rollback and then return the original error
                            .then( Mono.error( error ) ) );
            StepVerifier.create( records ).verifyErrorSatisfies( error ->
                    assertThat( error.getMessage(), containsString( "Invalid input" ) ) );
        } );

        StepVerifier.create( txRun ).expectNextCount( 1 ).verifyComplete();
    }

    @Test
    void shouldExposeStatementKeysForColumnsWithAliases()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( "RETURN 1 AS one, 2 AS two, 3 AS three, 4 AS five" );

            List<String> keys = await( result.keys() );
            assertEquals( Arrays.asList( "one", "two", "three", "five" ), keys );

            assertCanRollback( tx ); // you still need to rollback the tx as tx will not automatically closed
        } ) );
    }

    @Test
    void shouldExposeStatementKeysForColumnsWithoutAliases()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( "RETURN 1, 2, 3, 5" );

            List<String> keys = await( result.keys() );
            assertEquals( Arrays.asList( "1", "2", "3", "5" ), keys );

            assertCanRollback( tx ); // you still need to rollback the tx as tx will not automatically closed
        } ) );
    }

    @Test
    void shouldExposeResultSummaryForSimpleQuery()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            String query = "CREATE (p1:Person {name: $name1})-[:KNOWS]->(p2:Person {name: $name2}) RETURN p1, p2";
            Value params = parameters( "name1", "Bob", "name2", "John" );

            RxResult result = tx.run( query, params );
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
        } ) );
    }

    @Test
    void shouldExposeResultSummaryForExplainQuery()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            String query = "EXPLAIN MATCH (n) RETURN n";

            RxResult result = tx.run( query );
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
        } ) );
    }

    @Test
    void shouldExposeResultSummaryForProfileQuery()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            String query = "PROFILE MERGE (n {name: $name}) " +
                    "ON CREATE SET n.created = timestamp() " +
                    "ON MATCH SET n.counter = coalesce(n.counter, 0) + 1";

            Value params = parameters( "name", "Bob" );

            RxResult result = tx.run( query, params );
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
        } ) );
    }

    @Test
    void shouldCancelRecordStream()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( "UNWIND ['a', 'b', 'c'] AS x RETURN x" );

            Flux<String> abc = Flux.from( result.records() ).limitRate( 1 ).take( 1 ).map( record -> record.get( 0 ).asString() );
            StepVerifier.create( abc ).expectNext( "a" ).verifyComplete();

            assertCanRollback( tx ); // you still need to rollback the tx as tx will not automatically closed
        } ) );
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

        Mono<RxTransaction> rxTx = Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( "RETURN 'Hi!'" );
            Flux<Record> records = Flux.from( result.records() )
                    .doOnNext( record -> { throw e; } )
                    .concatWith( tx.commit() )
                    .onErrorResume( error -> Mono.from( tx.rollback() ).then( Mono.error( error ) ) );

            StepVerifier.create( records ).expectErrorSatisfies( error -> {
                assertEquals( e, error );
            } ).verify();
        } );

        StepVerifier.create( rxTx ).expectNextCount( 1 ).verifyComplete(); // we created a tx and did all the things inside.
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
        RxResult result = tx.run( "CREATE ()" );
        List<Map<String,Object>> maps = await( Flux.from( result.records() ).map( record -> record.get( 0 ).asMap() )  );
        assertEquals( 0, maps.size() );
        assertCanRollback( tx );
    }

    @Test
    void shouldConvertToTransformedListWithNonEmptyCursor()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxResult result = tx.run( "UNWIND ['a', 'b', 'c'] AS x RETURN x" );
        List<String> strings = await( Flux.from( result.records() ).map( record -> record.get( 0 ).asString() + "!" )  );

        assertEquals( Arrays.asList( "a!", "b!", "c!" ), strings );
        assertCanRollback( tx );
    }

    @Test
    void shouldFailWhenListTransformationFunctionFails()
    {
        RuntimeException e = new RuntimeException();
        
        Mono<RxTransaction> rxTx = Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( "RETURN 'Hi!'" );
            Flux<Object> records = Flux.from( result.records() )
                    .map( record -> { throw e; } )
                    .concatWith( tx.commit() )
                    .onErrorResume( error -> Mono.from( tx.rollback() ).then( Mono.error( error ) ) );

            StepVerifier.create( records ).expectErrorSatisfies( error -> {
                assertEquals( e, error );
            } ).verify();
        } );

        StepVerifier.create( rxTx ).expectNextCount( 1 ).verifyComplete(); // we created a tx and did all the things inside.
    }

    @Test
    void shouldFailToCommitWhenServerIsRestarted()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxResult result = tx.run( "RETURN 1" );

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
        RxResult result = tx.run( "MATCH (n:NoSuchLabel) RETURN n" );

        NoSuchElementException e = assertThrows( NoSuchElementException.class, () -> await( Flux.from( result.records() ).single() ) );
        assertThat( e.getMessage(), containsString( "Source was empty" ) );
        assertCanRollback( tx );
    }

    @Test
    void shouldFailSingleWithMultiRecordCursor()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxResult result = tx.run( "UNWIND ['a', 'b'] AS x RETURN x" );

        IndexOutOfBoundsException e = assertThrows( IndexOutOfBoundsException.class, () -> await( Flux.from( result.records() ).single() ) );
        assertThat( e.getMessage(), startsWith( "Source emitted more than one item" ) );
        assertCanRollback( tx );
    }

    @Test
    void shouldReturnSingleWithSingleRecordCursor()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxResult result = tx.run( "RETURN 'Hello!'" );

        Record record = await( Flux.from( result.records() ).single() );
        assertEquals( "Hello!", record.get( 0 ).asString() );
        assertCanRollback( tx );
    }

    @Test
    void shouldPropagateFailureFromFirstRecordInSingleAsync()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxResult result = tx.run( "UNWIND [0] AS x RETURN 10 / x" );

        ClientException e = assertThrows( ClientException.class, () -> await( Flux.from( result.records() ).single() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
        assertCanRollback( tx );
    }

    @Test
    void shouldPropagateFailureFromSecondRecordInSingleAsync()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxResult result = tx.run( "UNWIND [1, 0] AS x RETURN 10 / x" );

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
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanCommit( tx );

            CompletableFuture<Object> future = Mono.from( tx.commit() ).toFuture();
            assertTrue( future.isDone() );
        }));
    }

    @Test
    void shouldFailToCommitAfterRollback()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanRollback( tx );

            ClientException e = assertThrows( ClientException.class, () -> await( tx.commit() ) );
            assertEquals( "Can't commit, transaction has been rolled back", e.getMessage() );
        }));
    }

    @Test
    void shouldFailToCommitAfterTermination()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertFailToRunWrongStatement( tx );

            ClientException e = assertThrows( ClientException.class, () -> await( tx.commit() ) );
            assertThat( e.getMessage(), startsWith( "Transaction can't be committed" ) );
            assertCanRollback( tx );
        }));
    }

    @Test
    void shouldDoNothingWhenRolledBackSecondTime()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanRollback( tx );

            CompletableFuture<Object> future = Mono.from( tx.rollback() ).toFuture();
            assertTrue( future.isDone() );
        }));
    }

    @Test
    void shouldFailToRollbackAfterCommit()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertCanCommit( tx );

            ClientException e = assertThrows( ClientException.class, () -> await( tx.rollback() ) );
            assertEquals( "Can't rollback, transaction has been committed", e.getMessage() );
        }));
    }

    @Test
    void shouldRollbackAfterTermination()
    {
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertFailToRunWrongStatement( tx );
            assertCanRollback( tx );
        }));
    }

    @ParameterizedTest
    @MethodSource( "commit" )
    void shouldFailToRunQueryAfterCommit( boolean commit )
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxResult result = tx.run( "CREATE (:MyLabel)" );
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
        await( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            assertFailToRunWrongStatement( tx );

            ClientException e = assertThrows( ClientException.class, () -> await( tx.run( "CREATE (:MyOtherLabel)" ).records() ) );
            assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );

            assertCanRollback( tx );
        }));
    }

    @Test
    @EnabledOnNeo4jWith( BOOKMARKS )
    void shouldUpdateSessionBookmarkAfterCommit()
    {
        String bookmarkBefore = session.lastBookmark();

        await( Mono.from( session.beginTransaction()).doOnSuccess( tx -> {
            RxResult result = tx.run( "CREATE (:MyNode)" );
            await( Flux.from( result.records() )
                    .concatWith( tx.commit() )
                    .onErrorResume( error -> Mono.from( tx.rollback() ).then( Mono.error( error ) ) ) );
        } ) );

        String bookmarkAfter = session.lastBookmark();

        assertNotNull( bookmarkAfter );
        assertNotEquals( bookmarkBefore, bookmarkAfter );
    }

    @Test
    void shouldFailToCommitWhenQueriesFailAndErrorNotConsumed() throws InterruptedException
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );

        RxResult result1 = tx.run( "CREATE (:TestNode)" );
        RxResult result2 = tx.run( "CREATE (:TestNode)" );
        RxResult result3 = tx.run( "RETURN 10 / 0" );
        RxResult result4 = tx.run( "CREATE (:TestNode)" );

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

        RxResult result1 = tx.run( "RETURN 1" );
        RxResult result2 = tx.run( "RETURN 2" );
        RxResult result3 = tx.run( "RETURN 3" );
        RxResult result4 = tx.run( "RETURN 4" );

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
        RxResult result = tx.run( "RETURN 42 / 0" );
        await( result.keys() ); // always returns keys

        ClientException e = assertThrows( ClientException.class, () -> await( result.records() ) );
        assertThat( e.getMessage(), containsString( "/ by zero" ) );
        assertCanRollback( tx );
    }

    @Test
    void shouldFailToCommitWhenPullAllFailureIsConsumed()
    {
        RxTransaction tx = await( Mono.from( session.beginTransaction() ) );
        RxResult result = tx.run( "FOREACH (value IN [1,2, 'aaa'] | CREATE (:Person {name: 10 / value}))" );

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

        RxResult result = tx.run( "RETURN Wrong" );
        ClientException e = assertThrows( ClientException.class, () -> await( result.records() ) );
        assertThat( e.code(), containsString( "SyntaxError" ) );

        await( result.summary() );
        assertCanRollback( tx );
    }

    @Test
    void shouldHandleNestedQueries() throws Throwable
    {
        int size = 12555;

        Mono<RxTransaction> rxTx = Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( "UNWIND range(1, $size) AS x RETURN x", Collections.singletonMap( "size", size ) );
            Flux<Integer> nodeIds = Flux.from( result.records() )
                    .limitRate( 20 ) // batch size
                    .flatMap( record -> {
                        int x = record.get( "x" ).asInt();
                        RxResult innerResult = tx.run( "CREATE (n:Node {id: $x}) RETURN n.id", Collections.singletonMap( "x", x ) );
                        return innerResult.records();
                    } )
                    .concatWith( tx.commit() )
                    .onErrorResume( error -> Mono.from( tx.rollback() ).then( Mono.error( error ) ) )
                    .map( record -> record.get( 0 ).asInt() );

            StepVerifier.create( nodeIds ).expectNextCount( size ).verifyComplete();
        } );

        StepVerifier.create( rxTx ).expectNextCount( 1 ).verifyComplete();
    }

    private int countNodes( Object id )
    {
        RxResult result = session.run( "MATCH (n:Node {id: $id}) RETURN count(n)", parameters( "id", id ) );
        return await( Flux.from( result.records() ).single().map( record -> record.get( 0 ).asInt() ) );
    }

    private void testForEach( String query, int expectedSeenRecords )
    {
        Mono<RxTransaction> rxTx = Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( query );
            AtomicInteger recordsSeen = new AtomicInteger();
            Flux<ResultSummary> summary = Flux.from( result.records() )
                    .doOnNext( record -> recordsSeen.incrementAndGet() )
                    .then( Mono.from( result.summary() ) )
                    .doOnSuccess( s -> {
                        assertNotNull( s );
                        assertEquals( query, s.statement().text() );
                        assertEquals( emptyMap(), s.statement().parameters().asMap() );
                        assertEquals( expectedSeenRecords, recordsSeen.get() );
                    } )
                    .concatWith( tx.commit() )
                    .onErrorResume( error -> Mono.from( tx.rollback() ).then( Mono.error( error ) ) );

            StepVerifier.create( summary ).expectNextCount( 1 ).verifyComplete(); // we indeed get a summary.
        } );

        StepVerifier.create( rxTx ).expectNextCount( 1 ).verifyComplete(); // we created a tx and did all the things inside.
    }

    private <T> void testList( String query, List<T> expectedList )
    {
        List<Object> actualList = new ArrayList<>();

        Mono<RxTransaction> rxTx = Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( query );
            Flux<List<Record>> records = Flux.from( result.records() )
                    .collectList()
                    .concatWith( tx.commit() )
                    .onErrorResume( error -> Mono.from( tx.rollback() ).then( Mono.error( error ) ) );

            List<Record> allRecords = await( records.single() );
            for ( Record record : allRecords )
            {
                actualList.add( record.get( 0 ).asObject() );
            }
        } );

        StepVerifier.create( rxTx ).expectNextCount( 1 ).verifyComplete(); // we created a tx and did all the things inside.
        assertEquals( expectedList, actualList );
    }

    private void testConsume( String query )
    {
        Mono<RxTransaction> rxTx = Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            RxResult result = tx.run( query );
            Flux<Record> records = Flux.from( result.records() )
                    .next() // get at most one result to return
                    .concatWith( tx.commit() )
                    .onErrorResume( error -> Mono.from( tx.rollback() ).then( Mono.error( error ) ) );
            Record single = await( records.single( new InternalRecord( Collections.emptyList(), new Value[0] ) ) );
            assertNotNull( single );
        } );

        StepVerifier.create( rxTx ).expectNextCount( 1 ).verifyComplete(); // we created a tx and did all the things inside.
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
        RxResult result = tx.run( "CREATE (n:Node {id: 4242}) RETURN n" );

        Record record = await( Flux.from(result.records()).single() );

        Node node = record.get( 0 ).asNode();
        assertEquals( "Node", single( node.labels() ) );
        assertEquals( 4242, node.get( "id" ).asInt() );
    }

    private static void assertFailToRunWrongStatement( RxTransaction tx )
    {
        RxResult result = tx.run( "RETURN" );
        Exception e = assertThrows( Exception.class, () -> await( result.records() ) );
        assertThat( e, is( syntaxError( "Unexpected end of input" ) ) );
    }

    private void assertCanRunReturnOne( RxTransaction tx )
    {
        RxResult result = tx.run( "RETURN 42" );
        List<Record> records = await( result.records() );
        assertThat( records.size(), equalTo( 1 ) );
        Record record = records.get( 0 );
        assertEquals( 42, record.get( 0 ).asInt() );
    }
}
