/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.completedStage;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.internal.util.Matchers.arithmeticError;
import static org.neo4j.driver.internal.util.Matchers.containsResultAvailableAfterAndResultConsumedAfter;
import static org.neo4j.driver.internal.util.Matchers.syntaxError;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V3;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;
import static org.neo4j.driver.testutil.TestUtil.assertNoCircularReferences;
import static org.neo4j.driver.testutil.TestUtil.await;
import static org.neo4j.driver.testutil.TestUtil.awaitAll;

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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionCallback;
import org.neo4j.driver.async.AsyncTransactionContext;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.util.DisabledOnNeo4jWith;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.types.Node;

@ParallelizableIT
class AsyncSessionIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private AsyncSession session;

    @BeforeEach
    @SuppressWarnings("resource")
    void setUp() {
        session = neo4j.driver().session(AsyncSession.class);
    }

    @AfterEach
    void tearDown() {
        session.closeAsync();
    }

    @Test
    void shouldRunQueryWithEmptyResult() {
        var cursor = await(session.runAsync("CREATE (:Person)"));

        assertNull(await(cursor.nextAsync()));
    }

    @Test
    void shouldRunQueryWithSingleResult() {
        var cursor = await(session.runAsync("CREATE (p:Person {name: 'Nick Fury'}) RETURN p"));

        var record = await(cursor.nextAsync());
        assertNotNull(record);
        var node = record.get(0).asNode();
        assertEquals("Person", single(node.labels()));
        assertEquals("Nick Fury", node.get("name").asString());

        assertNull(await(cursor.nextAsync()));
    }

    @Test
    void shouldRunQueryWithMultipleResults() {
        var cursor = await(session.runAsync("UNWIND [1,2,3] AS x RETURN x"));

        var record1 = await(cursor.nextAsync());
        assertNotNull(record1);
        assertEquals(1, record1.get(0).asInt());

        var record2 = await(cursor.nextAsync());
        assertNotNull(record2);
        assertEquals(2, record2.get(0).asInt());

        var record3 = await(cursor.nextAsync());
        assertNotNull(record3);
        assertEquals(3, record3.get(0).asInt());

        assertNull(await(cursor.nextAsync()));
    }

    @Test
    void shouldFailForIncorrectQuery() {
        var e = assertThrows(Exception.class, () -> await(session.runAsync("RETURN")));

        assertThat(e, is(syntaxError()));
    }

    @Test
    void shouldFailWhenQueryFailsAtRuntime() {
        var cursor = await(session.runAsync("CYPHER runtime=interpreted UNWIND [1, 2, 0] AS x RETURN 10 / x"));

        var record1 = await(cursor.nextAsync());
        assertNotNull(record1);
        assertEquals(10, record1.get(0).asInt());

        var record2 = await(cursor.nextAsync());
        assertNotNull(record2);
        assertEquals(5, record2.get(0).asInt());

        var e = assertThrows(Exception.class, () -> await(cursor.nextAsync()));
        assertThat(e, is(arithmeticError()));
    }

    @Test
    void shouldAllowNestedQueries() {
        var cursor = await(session.runAsync("UNWIND [1, 2, 3] AS x CREATE (p:Person {id: x}) RETURN p"));

        var queriesExecuted = runNestedQueries(cursor);
        var futures = await(queriesExecuted);

        assertNotNull(futures);
        var futureResults = awaitAll(futures);
        assertEquals(7, futureResults.size());

        var personCursor = await(session.runAsync("MATCH (p:Person) RETURN p ORDER BY p.id"));

        List<Node> personNodes = new ArrayList<>();
        Record record;
        while ((record = await(personCursor.nextAsync())) != null) {
            personNodes.add(record.get(0).asNode());
        }
        assertEquals(3, personNodes.size());

        var node1 = personNodes.get(0);
        assertEquals(1, node1.get("id").asInt());
        assertEquals(10, node1.get("age").asInt());

        var node2 = personNodes.get(1);
        assertEquals(2, node2.get("id").asInt());
        assertEquals(20, node2.get("age").asInt());

        var node3 = personNodes.get(2);
        assertEquals(3, node3.get("id").asInt());
        assertEquals(30, personNodes.get(2).get("age").asInt());
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowMultipleAsyncRunsWithoutConsumingResults() {
        var queryCount = 13;
        var cursors = IntStream.range(0, queryCount)
                .mapToObj(i -> session.runAsync("CREATE (:Person)"))
                .collect(Collectors.toList());

        var records = awaitAll(cursors).stream().map(ResultCursor::nextAsync).collect(Collectors.toList());

        awaitAll(records);

        await(session.closeAsync());
        session = neo4j.driver().session(AsyncSession.class);

        var cursor = await(session.runAsync("MATCH (p:Person) RETURN count(p)"));
        var record = await(cursor.nextAsync());
        assertNotNull(record);
        assertEquals(queryCount, record.get(0).asInt());
    }

    @Test
    void shouldExposeQueryKeysForColumnsWithAliases() {
        var cursor = await(session.runAsync("RETURN 1 AS one, 2 AS two, 3 AS three, 4 AS five"));

        assertEquals(Arrays.asList("one", "two", "three", "five"), cursor.keys());
    }

    @Test
    void shouldExposeQueryKeysForColumnsWithoutAliases() {
        var cursor = await(session.runAsync("RETURN 1, 2, 3, 5"));

        assertEquals(Arrays.asList("1", "2", "3", "5"), cursor.keys());
    }

    @Test
    void shouldExposeResultSummaryForSimpleQuery() {
        var query = "CREATE (:Node {id: $id, name: $name})";
        var params = parameters("id", 1, "name", "TheNode");

        var cursor = await(session.runAsync(query, params));
        var summary = await(cursor.consumeAsync());

        assertEquals(new Query(query, params), summary.query());
        assertEquals(1, summary.counters().nodesCreated());
        assertEquals(1, summary.counters().labelsAdded());
        assertEquals(2, summary.counters().propertiesSet());
        assertEquals(0, summary.counters().relationshipsCreated());
        assertEquals(QueryType.WRITE_ONLY, summary.queryType());
        assertFalse(summary.hasPlan());
        assertFalse(summary.hasProfile());
        assertNull(summary.plan());
        assertNull(summary.profile());
        assertEquals(0, summary.notifications().size());
        assertThat(summary, containsResultAvailableAfterAndResultConsumedAfter());
    }

    @Test
    void shouldExposeResultSummaryForExplainQuery() {
        var query = "EXPLAIN CREATE (),() WITH * MATCH (n)-->(m) CREATE (n)-[:HI {id: 'id'}]->(m) RETURN n, m";

        var cursor = await(session.runAsync(query));
        var summary = await(cursor.consumeAsync());

        assertEquals(new Query(query), summary.query());
        assertEquals(0, summary.counters().nodesCreated());
        assertEquals(0, summary.counters().propertiesSet());
        assertEquals(0, summary.counters().relationshipsCreated());
        assertEquals(QueryType.READ_WRITE, summary.queryType());
        assertTrue(summary.hasPlan());
        assertFalse(summary.hasProfile());
        assertNotNull(summary.plan());
        // asserting on plan is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        var planAsString = summary.plan().toString().toLowerCase();
        assertThat(planAsString, containsString("create"));
        assertThat(planAsString, containsString("apply"));
        assertNull(summary.profile());
        assertEquals(0, summary.notifications().size());
        assertThat(summary, containsResultAvailableAfterAndResultConsumedAfter());
    }

    @Test
    void shouldExposeResultSummaryForProfileQuery() {
        var query = "PROFILE CREATE (:Node)-[:KNOWS]->(:Node) WITH * MATCH (n) RETURN n";

        var cursor = await(session.runAsync(query));
        var summary = await(cursor.consumeAsync());

        assertEquals(new Query(query), summary.query());
        assertEquals(2, summary.counters().nodesCreated());
        assertEquals(0, summary.counters().propertiesSet());
        assertEquals(1, summary.counters().relationshipsCreated());
        assertEquals(QueryType.READ_WRITE, summary.queryType());
        assertTrue(summary.hasPlan());
        assertTrue(summary.hasProfile());
        assertNotNull(summary.plan());
        assertNotNull(summary.profile());
        // asserting on profile is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        var profileAsString = summary.profile().toString().toLowerCase();
        assertThat(profileAsString, containsString("hits"));
        assertEquals(0, summary.notifications().size());
        assertThat(summary, containsResultAvailableAfterAndResultConsumedAfter());
    }

    @Test
    void shouldRunAsyncTransactionWithoutRetries() {
        var work = new InvocationTrackingWork("CREATE (:Apa) RETURN 42");
        var txStage = session.executeWriteAsync(work);

        var record = await(txStage);
        assertNotNull(record);
        assertEquals(42L, record.get(0).asLong());

        assertEquals(1, work.invocationCount());
        assertEquals(1, countNodesByLabel("Apa"));
    }

    @Test
    void shouldRunAsyncTransactionWithRetriesOnAsyncFailures() {
        var work = new InvocationTrackingWork("CREATE (:Node) RETURN 24")
                .withAsyncFailures(
                        new ServiceUnavailableException("Oh!"),
                        new SessionExpiredException("Ah!"),
                        new TransientException("Code", "Message"));

        var txStage = session.executeWriteAsync(work);

        var record = await(txStage);
        assertNotNull(record);
        assertEquals(24L, record.get(0).asLong());

        assertEquals(4, work.invocationCount());
        assertEquals(1, countNodesByLabel("Node"));
    }

    @Test
    void shouldRunAsyncTransactionWithRetriesOnSyncFailures() {
        var work = new InvocationTrackingWork("CREATE (:Test) RETURN 12")
                .withSyncFailures(
                        new TransientException("Oh!", "Deadlock!"),
                        new ServiceUnavailableException("Oh! Network Failure"));

        var txStage = session.executeWriteAsync(work);

        var record = await(txStage);
        assertNotNull(record);
        assertEquals(12L, record.get(0).asLong());

        assertEquals(3, work.invocationCount());
        assertEquals(1, countNodesByLabel("Test"));
    }

    @Test
    void shouldRunAsyncTransactionThatCanNotBeRetried() {
        var work = new InvocationTrackingWork("UNWIND [10, 5, 0] AS x CREATE (:Hi) RETURN 10/x");
        var txStage = session.executeWriteAsync(work);

        var e = assertThrows(ClientException.class, () -> await(txStage));
        assertNoCircularReferences(e);
        assertEquals(1, work.invocationCount());
        assertEquals(0, countNodesByLabel("Hi"));
    }

    @Test
    void shouldRunAsyncTransactionThatCanNotBeRetriedAfterATransientFailure() {
        // first throw TransientException directly from work, retry can happen afterwards
        // then return a future failed with DatabaseException, retry can't happen afterwards
        var work = new InvocationTrackingWork("CREATE (:Person) RETURN 1")
                .withSyncFailures(new TransientException("Oh!", "Deadlock!"))
                .withAsyncFailures(new DatabaseException("Oh!", "OutOfMemory!"));
        var txStage = session.executeWriteAsync(work);

        var e = assertThrows(DatabaseException.class, () -> await(txStage));

        assertEquals(1, e.getSuppressed().length);
        assertThat(e.getSuppressed()[0], instanceOf(TransientException.class));
        assertEquals(2, work.invocationCount());
        assertEquals(0, countNodesByLabel("Person"));
    }

    @Test
    void shouldPeekRecordFromCursor() {
        var cursor = await(session.runAsync("UNWIND [1, 2, 42] AS x RETURN x"));

        assertEquals(1, await(cursor.peekAsync()).get(0).asInt());
        assertEquals(1, await(cursor.peekAsync()).get(0).asInt());
        assertEquals(1, await(cursor.peekAsync()).get(0).asInt());

        assertEquals(1, await(cursor.nextAsync()).get(0).asInt());

        assertEquals(2, await(cursor.peekAsync()).get(0).asInt());
        assertEquals(2, await(cursor.peekAsync()).get(0).asInt());

        assertEquals(2, await(cursor.nextAsync()).get(0).asInt());

        assertEquals(42, await(cursor.nextAsync()).get(0).asInt());

        assertNull(await(cursor.peekAsync()));
        assertNull(await(cursor.nextAsync()));
    }

    @Test
    void shouldForEachWithEmptyCursor() {
        testForEach("CREATE ()", 0);
    }

    @Test
    void shouldForEachWithNonEmptyCursor() {
        testForEach("UNWIND range(1, 100000) AS x RETURN x", 100000);
    }

    @Test
    void shouldFailForEachWhenActionFails() {
        var cursor = await(session.runAsync("RETURN 42"));
        var error = new IOException("Hi");

        var e = assertThrows(
                IOException.class,
                () -> await(cursor.forEachAsync(record -> {
                    throw new CompletionException(error);
                })));
        assertEquals(error, e);
    }

    @Test
    void shouldConvertToListWithEmptyCursor() {
        testList("MATCH (n:NoSuchLabel) RETURN n", Collections.emptyList());
    }

    @Test
    void shouldConvertToListWithNonEmptyCursor() {
        testList(
                "UNWIND range(1, 100, 10) AS x RETURN x",
                Arrays.asList(1L, 11L, 21L, 31L, 41L, 51L, 61L, 71L, 81L, 91L));
    }

    @Test
    void shouldConvertToTransformedListWithEmptyCursor() {
        var cursor = await(session.runAsync("CREATE ()"));
        var strings = await(cursor.listAsync(record -> "Hi!"));
        assertEquals(0, strings.size());
    }

    @Test
    void shouldConvertToTransformedListWithNonEmptyCursor() {
        var cursor = await(session.runAsync("UNWIND [1,2,3] AS x RETURN x"));
        var ints = await(cursor.listAsync(record -> record.get(0).asInt() + 1));
        assertEquals(Arrays.asList(2, 3, 4), ints);
    }

    @Test
    void shouldFailWhenListTransformationFunctionFails() {
        var cursor = await(session.runAsync("RETURN 42"));
        var error = new RuntimeException("Hi!");

        var e = assertThrows(
                RuntimeException.class,
                () -> await(cursor.listAsync(record -> {
                    throw error;
                })));
        assertEquals(error, e);
    }

    @Test
    void shouldFailSingleWithEmptyCursor() {
        var cursor = await(session.runAsync("CREATE ()"));

        var e = assertThrows(NoSuchRecordException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), containsString("result is empty"));
    }

    @Test
    void shouldFailSingleWithMultiRecordCursor() {
        var cursor = await(session.runAsync("UNWIND [1, 2, 3] AS x RETURN x"));

        var e = assertThrows(NoSuchRecordException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), startsWith("Expected a result with a single record"));
    }

    @Test
    void shouldReturnSingleWithSingleRecordCursor() {
        var cursor = await(session.runAsync("RETURN 42"));

        var record = await(cursor.singleAsync());

        assertEquals(42, record.get(0).asInt());
    }

    @Test
    void shouldPropagateFailureFromFirstRecordInSingleAsync() {
        var cursor = await(session.runAsync("UNWIND [0] AS x RETURN 10 / x"));

        var e = assertThrows(ClientException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), containsString("/ by zero"));
    }

    @Test
    void shouldNotPropagateFailureFromSecondRecordInSingleAsync() {
        var cursor = await(session.runAsync("UNWIND [1, 0] AS x RETURN 10 / x"));

        var e = assertThrows(ClientException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), containsString("/ by zero"));
    }

    @Test
    void shouldConsumeEmptyCursor() {
        testConsume("CREATE ()");
    }

    @Test
    void shouldConsumeNonEmptyCursor() {
        testConsume("UNWIND [42, 42] AS x RETURN x");
    }

    @Test
    @DisabledOnNeo4jWith(BOLT_V3)
    @SuppressWarnings("resource")
    void shouldRunAfterBeginTxFailureOnBookmark() {
        var illegalBookmark = Bookmark.from("Illegal Bookmark");
        session = neo4j.driver()
                .session(
                        AsyncSession.class,
                        builder().withBookmarks(illegalBookmark).build());

        assertThrows(ClientException.class, () -> await(session.beginTransactionAsync()));

        var cursor = await(session.runAsync("RETURN 'Hello!'"));
        var record = await(cursor.singleAsync());
        assertEquals("Hello!", record.get(0).asString());
    }

    @Test
    @SuppressWarnings("resource")
    void shouldNotBeginTxAfterBeginTxFailureOnBookmark() {
        var illegalBookmark = Bookmark.from("Illegal Bookmark");
        session = neo4j.driver()
                .session(
                        AsyncSession.class,
                        builder().withBookmarks(illegalBookmark).build());
        assertThrows(ClientException.class, () -> await(session.beginTransactionAsync()));
        assertThrows(ClientException.class, () -> await(session.beginTransactionAsync()));
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V3)
    @SuppressWarnings("resource")
    void shouldNotRunAfterBeginTxFailureOnBookmark() {
        var illegalBookmark = Bookmark.from("Illegal Bookmark");
        session = neo4j.driver()
                .session(
                        AsyncSession.class,
                        builder().withBookmarks(illegalBookmark).build());
        assertThrows(ClientException.class, () -> await(session.beginTransactionAsync()));
        assertThrows(ClientException.class, () -> await(session.runAsync("RETURN 'Hello!'")));
    }

    @Test
    void shouldExecuteReadTransactionUntilSuccessWhenWorkThrows() {
        var maxFailures = 1;

        var result = session.executeReadAsync(new AsyncTransactionCallback<CompletionStage<Integer>>() {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<Integer> execute(AsyncTransactionContext tx) {
                if (failures.getAndIncrement() < maxFailures) {
                    throw new SessionExpiredException("Oh!");
                }
                return tx.runAsync("UNWIND range(1, 10) AS x RETURN count(x)")
                        .thenCompose(ResultCursor::singleAsync)
                        .thenApply(record -> record.get(0).asInt());
            }
        });

        assertEquals(10, await(result).intValue());
    }

    @Test
    void shouldExecuteWriteTransactionUntilSuccessWhenWorkThrows() {
        var maxFailures = 2;

        var result = session.executeWriteAsync(new AsyncTransactionCallback<CompletionStage<Integer>>() {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<Integer> execute(AsyncTransactionContext tx) {
                if (failures.getAndIncrement() < maxFailures) {
                    throw new ServiceUnavailableException("Oh!");
                }
                return tx.runAsync("CREATE (n1:TestNode), (n2:TestNode) RETURN 2")
                        .thenCompose(ResultCursor::singleAsync)
                        .thenApply(record -> record.get(0).asInt());
            }
        });

        assertEquals(2, await(result).intValue());
        assertEquals(2, countNodesByLabel("TestNode"));
    }

    @Test
    void shouldExecuteReadTransactionUntilSuccessWhenWorkFails() {
        var maxFailures = 3;

        var result = session.executeReadAsync(new AsyncTransactionCallback<CompletionStage<Integer>>() {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<Integer> execute(AsyncTransactionContext tx) {
                return tx.runAsync("RETURN 42")
                        .thenCompose(ResultCursor::singleAsync)
                        .thenApply(record -> record.get(0).asInt())
                        .thenCompose(result -> {
                            if (failures.getAndIncrement() < maxFailures) {
                                return failedFuture(new TransientException("A", "B"));
                            }
                            return completedFuture(result);
                        });
            }
        });

        assertEquals(42, await(result).intValue());
    }

    @Test
    void shouldExecuteWriteTransactionUntilSuccessWhenWorkFails() {
        var maxFailures = 2;

        var result = session.executeWriteAsync(new AsyncTransactionCallback<CompletionStage<String>>() {
            final AtomicInteger failures = new AtomicInteger();

            @Override
            public CompletionStage<String> execute(AsyncTransactionContext tx) {
                return tx.runAsync("CREATE (:MyNode) RETURN 'Hello'")
                        .thenCompose(ResultCursor::singleAsync)
                        .thenApply(record -> record.get(0).asString())
                        .thenCompose(result -> {
                            if (failures.getAndIncrement() < maxFailures) {
                                return failedFuture(new ServiceUnavailableException("Hi"));
                            }
                            return completedFuture(result);
                        });
            }
        });

        assertEquals("Hello", await(result));
        assertEquals(1, countNodesByLabel("MyNode"));
    }

    @Test
    void shouldNotPropagateRunFailureWhenClosed() {
        session.runAsync("RETURN 1 * \"x\"");

        await(session.closeAsync());
    }

    @Test
    void shouldPropagateRunFailureImmediately() {
        var e = assertThrows(ClientException.class, () -> await(session.runAsync("RETURN 1 * \"x\"")));

        assertThat(e.getMessage(), containsString("Type mismatch"));
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    void shouldNotPropagateFailureWhenStreamingIsCancelled() {
        session.runAsync("UNWIND range(20000, 0, -1) AS x RETURN 10 / x");

        await(session.closeAsync());
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    void shouldNotPropagateBlockedPullAllFailureWhenClosed() {
        await(session.runAsync("UNWIND range(20000, 0, -1) AS x RETURN 10 / x"));

        await(session.closeAsync());
    }

    @Test
    void shouldCloseCleanlyWhenRunErrorConsumed() {
        var e = assertThrows(ClientException.class, () -> await(session.runAsync("SomeWrongQuery")));

        assertThat(e.getMessage(), startsWith("Invalid input"));
        assertNull(await(session.closeAsync()));
    }

    @Test
    void shouldCloseCleanlyWhenPullAllErrorConsumed() {
        var cursor = await(session.runAsync("UNWIND range(10, 0, -1) AS x RETURN 1 / x"));

        var e = assertThrows(ClientException.class, () -> await(cursor.consumeAsync()));
        assertThat(e.getMessage(), containsString("/ by zero"));
        assertNull(await(session.closeAsync()));
    }

    @Test
    void shouldNotPropagateFailureInCloseFromPreviousRun() {
        await(session.runAsync("CREATE ()"));
        await(session.runAsync("CREATE ()"));
        await(session.runAsync("CREATE ()"));
        assertThrows(ClientException.class, () -> await(session.runAsync("RETURN invalid")));

        await(session.closeAsync());
    }

    @Test
    void shouldCloseCleanlyAfterFailure() {
        var runWithOpenTx = session.beginTransactionAsync().thenCompose(tx -> session.runAsync("RETURN 1"));

        var e = assertThrows(ClientException.class, () -> await(runWithOpenTx));
        assertThat(e.getMessage(), startsWith("Queries cannot be run directly on a session with an open transaction"));

        await(session.closeAsync());
    }

    @Test
    void shouldPropagateFailureFromFirstIllegalQuery() {
        var allQueries = session.runAsync("CREATE (:Node1)")
                .thenCompose(ignore -> session.runAsync("CREATE (:Node2)"))
                .thenCompose(ignore -> session.runAsync("RETURN invalid"))
                .thenCompose(ignore -> session.runAsync("CREATE (:Node3)"));

        var e = assertThrows(ClientException.class, () -> await(allQueries));
        assertThat(e, is(syntaxError("Variable `invalid` not defined")));

        assertEquals(1, countNodesByLabel("Node1"));
        assertEquals(1, countNodesByLabel("Node2"));
        assertEquals(0, countNodesByLabel("Node3"));
    }

    @Test
    void shouldAllowReturningNullFromAsyncTransactionFunction() {
        var readResult = session.executeReadAsync(tx -> null);
        assertNull(await(readResult));

        var writeResult = session.executeWriteAsync(tx -> null);
        assertNull(await(writeResult));
    }

    @ParameterizedTest
    @MethodSource("managedTransactionsReturningResultCursorStage")
    void shouldErrorWhenResultCursorIsReturned(Function<AsyncSession, CompletionStage<ResultCursor>> fn) {
        var error = assertThrows(ClientException.class, () -> await(fn.apply(session)));
        assertEquals(
                "org.neo4j.driver.async.ResultCursor is not a valid return value, it should be consumed before producing a return value",
                error.getMessage());
        await(session.closeAsync());
    }

    static List<Function<AsyncSession, CompletionStage<ResultCursor>>> managedTransactionsReturningResultCursorStage() {
        return List.of(
                session -> session.executeWriteAsync(tx -> tx.runAsync("RETURN 1")),
                session -> session.executeReadAsync(tx -> tx.runAsync("RETURN 1")),
                session -> session.executeWriteAsync(tx -> tx.runAsync("RETURN 1")),
                session -> session.executeReadAsync(tx -> tx.runAsync("RETURN 1")));
    }

    private Future<List<CompletionStage<Record>>> runNestedQueries(ResultCursor inputCursor) {
        var resultFuture = new CompletableFuture<List<CompletionStage<Record>>>();
        runNestedQueries(inputCursor, new ArrayList<>(), resultFuture);
        return resultFuture;
    }

    private void runNestedQueries(
            ResultCursor inputCursor,
            List<CompletionStage<Record>> stages,
            CompletableFuture<List<CompletionStage<Record>>> resultFuture) {
        final var recordResponse = inputCursor.nextAsync();
        stages.add(recordResponse);

        recordResponse.whenComplete((record, error) -> {
            if (error != null) {
                resultFuture.completeExceptionally(error);
            } else if (record != null) {
                runNestedQuery(inputCursor, record, stages, resultFuture);
            } else {
                resultFuture.complete(stages);
            }
        });
    }

    private void runNestedQuery(
            ResultCursor inputCursor,
            Record record,
            List<CompletionStage<Record>> stages,
            CompletableFuture<List<CompletionStage<Record>>> resultFuture) {
        var node = record.get(0).asNode();
        var id = node.get("id").asLong();
        var age = id * 10;

        var response = session.runAsync(
                "MATCH (p:Person {id: $id}) SET p.age = $age RETURN p", parameters("id", id, "age", age));

        response.thenCompose(ResultCursor::nextAsync).whenComplete((entry, error) -> {
            if (error != null) {
                resultFuture.completeExceptionally(Futures.completionExceptionCause(error));
            } else {
                stages.add(completedStage(entry));
                runNestedQueries(inputCursor, stages, resultFuture);
            }
        });
    }

    private long countNodesByLabel(String label) {
        var countStage = session.runAsync("MATCH (n:" + label + ") RETURN count(n)")
                .thenCompose(ResultCursor::singleAsync)
                .thenApply(record -> record.get(0).asLong());

        return await(countStage);
    }

    private void testForEach(String query, int expectedSeenRecords) {
        var cursor = await(session.runAsync(query));

        var recordsSeen = new AtomicInteger();
        var forEachDone = cursor.forEachAsync(record -> recordsSeen.incrementAndGet());
        var summary = await(forEachDone);

        assertNotNull(summary);
        assertEquals(query, summary.query().text());
        assertEquals(emptyMap(), summary.query().parameters().asMap());
        assertEquals(expectedSeenRecords, recordsSeen.get());
    }

    private <T> void testList(String query, List<T> expectedList) {
        var cursor = await(session.runAsync(query));
        var records = await(cursor.listAsync());
        var actualList =
                records.stream().map(record -> record.get(0).asObject()).collect(Collectors.toList());
        assertEquals(expectedList, actualList);
    }

    private void testConsume(String query) {
        var cursor = await(session.runAsync(query));
        var summary = await(cursor.consumeAsync());

        assertNotNull(summary);
        assertEquals(query, summary.query().text());
        assertEquals(emptyMap(), summary.query().parameters().asMap());

        // no records should be available, they should all be consumed
        assertThrows(ResultConsumedException.class, () -> await(cursor.nextAsync()));
    }

    private static class InvocationTrackingWork implements AsyncTransactionCallback<CompletionStage<Record>> {
        final String query;
        final AtomicInteger invocationCount;

        Iterator<RuntimeException> asyncFailures = emptyIterator();
        Iterator<RuntimeException> syncFailures = emptyIterator();

        InvocationTrackingWork(String query) {
            this.query = query;
            this.invocationCount = new AtomicInteger();
        }

        InvocationTrackingWork withAsyncFailures(RuntimeException... failures) {
            asyncFailures = Arrays.asList(failures).iterator();
            return this;
        }

        InvocationTrackingWork withSyncFailures(RuntimeException... failures) {
            syncFailures = Arrays.asList(failures).iterator();
            return this;
        }

        int invocationCount() {
            return invocationCount.get();
        }

        @Override
        public CompletionStage<Record> execute(AsyncTransactionContext tx) {
            invocationCount.incrementAndGet();

            if (syncFailures.hasNext()) {
                throw syncFailures.next();
            }

            var resultFuture = new CompletableFuture<Record>();

            tx.runAsync(query)
                    .whenComplete((cursor, error) ->
                            processQueryResult(cursor, Futures.completionExceptionCause(error), resultFuture));

            return resultFuture;
        }

        private void processQueryResult(ResultCursor cursor, Throwable error, CompletableFuture<Record> resultFuture) {
            if (error != null) {
                resultFuture.completeExceptionally(error);
                return;
            }

            cursor.nextAsync()
                    .whenComplete((record, fetchError) ->
                            processFetchResult(record, Futures.completionExceptionCause(fetchError), resultFuture));
        }

        private void processFetchResult(Record record, Throwable error, CompletableFuture<Record> resultFuture) {
            if (error != null) {
                resultFuture.completeExceptionally(error);
                return;
            }

            if (record == null) {
                resultFuture.completeExceptionally(new AssertionError("Record not available"));
                return;
            }

            if (asyncFailures.hasNext()) {
                resultFuture.completeExceptionally(asyncFailures.next());
            } else {
                resultFuture.complete(record);
            }
        }
    }
}
