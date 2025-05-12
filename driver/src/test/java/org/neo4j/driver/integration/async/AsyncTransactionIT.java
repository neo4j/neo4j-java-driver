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

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.internal.util.Matchers.containsResultAvailableAfterAndResultConsumedAfter;
import static org.neo4j.driver.internal.util.Matchers.syntaxError;
import static org.neo4j.driver.testutil.TestUtil.assertNoCircularReferences;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class AsyncTransactionIT {
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
    void shouldBePossibleToCommitEmptyTx() {
        var bookmarksBefore = session.lastBookmarks();

        var tx = await(session.beginTransactionAsync());
        assertThat(await(tx.commitAsync()), is(nullValue()));

        var bookmarksAfter = session.lastBookmarks();

        assertNotNull(bookmarksAfter);
        assertNotEquals(bookmarksBefore, bookmarksAfter);
    }

    @Test
    void shouldBePossibleToRollbackEmptyTx() {
        var bookmarksBefore = session.lastBookmarks();

        var tx = await(session.beginTransactionAsync());
        assertThat(await(tx.rollbackAsync()), is(nullValue()));

        var bookmarksAfter = session.lastBookmarks();
        assertEquals(bookmarksBefore, bookmarksAfter);
    }

    @Test
    void shouldBePossibleToRunSingleQueryAndCommit() {
        var tx = await(session.beginTransactionAsync());

        var cursor = await(tx.runAsync("CREATE (n:Node {id: 42}) RETURN n"));

        var record = await(cursor.nextAsync());
        assertNotNull(record);
        var node = record.get(0).asNode();
        assertEquals("Node", single(node.labels()));
        assertEquals(42, node.get("id").asInt());
        assertNull(await(cursor.nextAsync()));

        assertNull(await(tx.commitAsync()));
        assertEquals(1, countNodes(42));
    }

    @Test
    void shouldBePossibleToRunSingleQueryAndRollback() {
        var tx = await(session.beginTransactionAsync());

        var cursor = await(tx.runAsync("CREATE (n:Node {id: 4242}) RETURN n"));
        var record = await(cursor.nextAsync());
        assertNotNull(record);
        var node = record.get(0).asNode();
        assertEquals("Node", single(node.labels()));
        assertEquals(4242, node.get("id").asInt());
        assertNull(await(cursor.nextAsync()));

        assertNull(await(tx.rollbackAsync()));
        assertEquals(0, countNodes(4242));
    }

    @Test
    void shouldBePossibleToRunMultipleQueriesAndCommit() {
        var tx = await(session.beginTransactionAsync());

        var cursor1 = await(tx.runAsync("CREATE (n:Node {id: 1})"));
        assertNull(await(cursor1.nextAsync()));

        var cursor2 = await(tx.runAsync("CREATE (n:Node {id: 2})"));
        assertNull(await(cursor2.nextAsync()));

        var cursor3 = await(tx.runAsync("CREATE (n:Node {id: 2})"));
        assertNull(await(cursor3.nextAsync()));

        assertNull(await(tx.commitAsync()));
        assertEquals(1, countNodes(1));
        assertEquals(2, countNodes(2));
    }

    @Test
    void shouldBePossibleToRunMultipleQueriesAndCommitWithoutWaiting() {
        var tx = await(session.beginTransactionAsync());

        tx.runAsync("CREATE (n:Node {id: 1})");
        tx.runAsync("CREATE (n:Node {id: 2})");
        tx.runAsync("CREATE (n:Node {id: 1})");

        assertNull(await(tx.commitAsync()));
        assertEquals(1, countNodes(2));
        assertEquals(2, countNodes(1));
    }

    @Test
    void shouldBePossibleToRunMultipleQueriesAndRollback() {
        var tx = await(session.beginTransactionAsync());

        var cursor1 = await(tx.runAsync("CREATE (n:Node {id: 1})"));
        assertNull(await(cursor1.nextAsync()));

        var cursor2 = await(tx.runAsync("CREATE (n:Node {id: 42})"));
        assertNull(await(cursor2.nextAsync()));

        assertNull(await(tx.rollbackAsync()));
        assertEquals(0, countNodes(1));
        assertEquals(0, countNodes(42));
    }

    @Test
    void shouldBePossibleToRunMultipleQueriesAndRollbackWithoutWaiting() {
        var tx = await(session.beginTransactionAsync());

        tx.runAsync("CREATE (n:Node {id: 1})");
        tx.runAsync("CREATE (n:Node {id: 42})");

        assertNull(await(tx.rollbackAsync()));
        assertEquals(0, countNodes(1));
        assertEquals(0, countNodes(42));
    }

    @Test
    void shouldFailToCommitAfterSingleWrongQuery() {
        var tx = await(session.beginTransactionAsync());

        var e = assertThrows(Exception.class, () -> await(tx.runAsync("RETURN")));
        assertThat(e, is(syntaxError()));

        assertThrows(ClientException.class, () -> await(tx.commitAsync()));
    }

    @Test
    void shouldAllowRollbackAfterSingleWrongQuery() {
        var tx = await(session.beginTransactionAsync());

        var e = assertThrows(Exception.class, () -> await(tx.runAsync("RETURN")));
        assertThat(e, is(syntaxError()));
        assertThat(await(tx.rollbackAsync()), is(nullValue()));
    }

    @Test
    void shouldFailToCommitAfterCoupleCorrectAndSingleWrongQuery() {
        var tx = await(session.beginTransactionAsync());

        var cursor1 = await(tx.runAsync("CREATE (n:Node) RETURN n"));
        var record1 = await(cursor1.nextAsync());
        assertNotNull(record1);
        assertTrue(record1.get(0).asNode().hasLabel("Node"));

        var cursor2 = await(tx.runAsync("RETURN 42"));
        var record2 = await(cursor2.nextAsync());
        assertNotNull(record2);
        assertEquals(42, record2.get(0).asInt());

        var e = assertThrows(Exception.class, () -> await(tx.runAsync("RETURN")));
        assertThat(e, is(syntaxError()));

        assertThrows(ClientException.class, () -> await(tx.commitAsync()));
    }

    @Test
    void shouldAllowRollbackAfterCoupleCorrectAndSingleWrongQuery() {
        var tx = await(session.beginTransactionAsync());

        var cursor1 = await(tx.runAsync("RETURN 4242"));
        var record1 = await(cursor1.nextAsync());
        assertNotNull(record1);
        assertEquals(4242, record1.get(0).asInt());

        var cursor2 = await(tx.runAsync("CREATE (n:Node) DELETE n RETURN 42"));
        var record2 = await(cursor2.nextAsync());
        assertNotNull(record2);
        assertEquals(42, record2.get(0).asInt());

        var e = assertThrows(Exception.class, () -> await(tx.runAsync("RETURN")));
        assertThat(e, is(syntaxError()));
        assertThat(await(tx.rollbackAsync()), is(nullValue()));
    }

    @Test
    void shouldNotAllowNewQueriesAfterAnIncorrectQuery() {
        var tx = await(session.beginTransactionAsync());

        var e1 = assertThrows(Exception.class, () -> await(tx.runAsync("RETURN")));
        assertThat(e1, is(syntaxError()));

        var e2 = assertThrows(ClientException.class, () -> tx.runAsync("CREATE ()"));
        assertThat(e2.getMessage(), startsWith("Cannot run more queries in this transaction"));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldFailBoBeginTxWithInvalidBookmark() {
        var session = neo4j.driver()
                .session(
                        AsyncSession.class,
                        builder()
                                .withBookmarks(Bookmark.from("InvalidBookmark"))
                                .build());

        var e = assertThrows(ClientException.class, () -> await(session.beginTransactionAsync()));
        assertTrue(e.getMessage().contains("InvalidBookmark")
                || e.getMessage().contains("Parsing of supplied bookmarks failed"));
    }

    @Test
    void shouldFailToCommitWhenCommitted() {
        var tx = await(session.beginTransactionAsync());
        tx.runAsync("CREATE ()");
        assertNull(await(tx.commitAsync()));

        // should not be possible to commit after commit
        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertThat(e.getMessage(), containsString("transaction has been committed"));
    }

    @Test
    void shouldFailToRollbackWhenRolledBack() {
        var tx = await(session.beginTransactionAsync());
        tx.runAsync("CREATE ()");
        assertNull(await(tx.rollbackAsync()));

        // should not be possible to rollback after rollback
        var e = assertThrows(ClientException.class, () -> await(tx.rollbackAsync()));
        assertThat(e.getMessage(), containsString("transaction has been rolled back"));
    }

    @Test
    void shouldFailToCommitWhenRolledBack() {
        var tx = await(session.beginTransactionAsync());
        tx.runAsync("CREATE ()");
        assertNull(await(tx.rollbackAsync()));

        // should not be possible to commit after rollback
        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertThat(e.getMessage(), containsString("transaction has been rolled back"));
    }

    @Test
    void shouldFailToRollbackWhenCommitted() {
        var tx = await(session.beginTransactionAsync());
        tx.runAsync("CREATE ()");
        assertNull(await(tx.commitAsync()));

        // should not be possible to rollback after commit
        var e = assertThrows(ClientException.class, () -> await(tx.rollbackAsync()));
        assertThat(e.getMessage(), containsString("transaction has been committed"));
    }

    @Test
    void shouldExposeQueryKeysForColumnsWithAliases() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("RETURN 1 AS one, 2 AS two, 3 AS three, 4 AS five"));

        assertEquals(Arrays.asList("one", "two", "three", "five"), cursor.keys());
    }

    @Test
    void shouldExposeQueryKeysForColumnsWithoutAliases() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("RETURN 1, 2, 3, 5"));

        assertEquals(Arrays.asList("1", "2", "3", "5"), cursor.keys());
    }

    @Test
    void shouldExposeResultSummaryForSimpleQuery() {
        var query = "CREATE (p1:Person {name: $name1})-[:KNOWS]->(p2:Person {name: $name2}) RETURN p1, p2";
        var params = parameters("name1", "Bob", "name2", "John");

        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync(query, params));
        var summary = await(cursor.consumeAsync());

        assertEquals(new Query(query, params), summary.query());
        assertEquals(2, summary.counters().nodesCreated());
        assertEquals(2, summary.counters().labelsAdded());
        assertEquals(2, summary.counters().propertiesSet());
        assertEquals(1, summary.counters().relationshipsCreated());
        assertEquals(QueryType.READ_WRITE, summary.queryType());
        assertFalse(summary.hasPlan());
        assertFalse(summary.hasProfile());
        assertNull(summary.plan());
        assertNull(summary.profile());
        assertEquals(0, summary.notifications().size());
        assertThat(summary, containsResultAvailableAfterAndResultConsumedAfter());
    }

    @Test
    void shouldExposeResultSummaryForExplainQuery() {
        var query = "EXPLAIN MATCH (n) RETURN n";

        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync(query));
        var summary = await(cursor.consumeAsync());

        assertEquals(new Query(query), summary.query());
        assertEquals(0, summary.counters().nodesCreated());
        assertEquals(0, summary.counters().propertiesSet());
        assertEquals(QueryType.READ_ONLY, summary.queryType());
        assertTrue(summary.hasPlan());
        assertFalse(summary.hasProfile());
        assertNotNull(summary.plan());
        // asserting on plan is a bit fragile and can break when server side changes or with different
        // server versions; that is why do fuzzy assertions in this test based on string content
        assertThat(summary.plan().toString().toLowerCase(), containsString("scan"));
        assertNull(summary.profile());
        assertEquals(0, summary.notifications().size());
        assertThat(summary, containsResultAvailableAfterAndResultConsumedAfter());
    }

    @Test
    void shouldExposeResultSummaryForProfileQuery() {
        var query = "PROFILE MERGE (n {name: $name}) " + "ON CREATE SET n.created = timestamp() "
                + "ON MATCH SET n.counter = coalesce(n.counter, 0) + 1";

        var params = parameters("name", "Bob");

        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync(query, params));
        var summary = await(cursor.consumeAsync());

        assertEquals(new Query(query, params), summary.query());
        assertEquals(1, summary.counters().nodesCreated());
        assertEquals(2, summary.counters().propertiesSet());
        assertEquals(0, summary.counters().relationshipsCreated());
        assertEquals(QueryType.WRITE_ONLY, summary.queryType());
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
    void shouldPeekRecordFromCursor() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("UNWIND ['a', 'b', 'c'] AS x RETURN x"));

        assertEquals("a", await(cursor.peekAsync()).get(0).asString());
        assertEquals("a", await(cursor.peekAsync()).get(0).asString());

        assertEquals("a", await(cursor.nextAsync()).get(0).asString());

        assertEquals("b", await(cursor.peekAsync()).get(0).asString());
        assertEquals("b", await(cursor.peekAsync()).get(0).asString());
        assertEquals("b", await(cursor.peekAsync()).get(0).asString());

        assertEquals("b", await(cursor.nextAsync()).get(0).asString());
        assertEquals("c", await(cursor.nextAsync()).get(0).asString());

        assertNull(await(cursor.peekAsync()));
        assertNull(await(cursor.nextAsync()));

        await(tx.rollbackAsync());
    }

    @Test
    void shouldForEachWithEmptyCursor() {
        testForEach("MATCH (n:SomeReallyStrangeLabel) RETURN n", 0);
    }

    @Test
    void shouldForEachWithNonEmptyCursor() {
        testForEach("UNWIND range(1, 12555) AS x CREATE (n:Node {id: x}) RETURN n", 12555);
    }

    @Test
    void shouldFailForEachWhenActionFails() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("RETURN 'Hi!'"));
        var error = new RuntimeException();

        var e = assertThrows(
                RuntimeException.class,
                () -> await(cursor.forEachAsync(record -> {
                    throw error;
                })));
        assertEquals(error, e);
    }

    @Test
    void shouldConvertToListWithEmptyCursor() {
        testList("CREATE (:Person)-[:KNOWS]->(:Person)", Collections.emptyList());
    }

    @Test
    void shouldConvertToListWithNonEmptyCursor() {
        testList("UNWIND [1, '1', 2, '2', 3, '3'] AS x RETURN x", Arrays.asList(1L, "1", 2L, "2", 3L, "3"));
    }

    @Test
    void shouldConvertToTransformedListWithEmptyCursor() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("CREATE ()"));
        var maps = await(cursor.listAsync(record -> record.get(0).asMap()));
        assertEquals(0, maps.size());
    }

    @Test
    void shouldConvertToTransformedListWithNonEmptyCursor() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("UNWIND ['a', 'b', 'c'] AS x RETURN x"));
        var strings = await(cursor.listAsync(record -> record.get(0).asString() + "!"));
        assertEquals(Arrays.asList("a!", "b!", "c!"), strings);
    }

    @Test
    void shouldFailWhenListTransformationFunctionFails() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("RETURN 'Hello'"));
        var error = new IOException("World");

        var e = assertThrows(
                Exception.class,
                () -> await(cursor.listAsync(record -> {
                    throw new CompletionException(error);
                })));
        assertEquals(error, e);
    }

    @Test
    void shouldFailToCommitWhenServerIsRestarted() {
        var tx = await(session.beginTransactionAsync());

        await(tx.runAsync("CREATE ()"));

        neo4j.stopProxy();

        assertThrows(ServiceUnavailableException.class, () -> await(tx.commitAsync()));
    }

    @Test
    void shouldFailSingleWithEmptyCursor() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("MATCH (n:NoSuchLabel) RETURN n"));

        var e = assertThrows(NoSuchRecordException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), containsString("result is empty"));
    }

    @Test
    void shouldFailSingleWithMultiRecordCursor() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("UNWIND ['a', 'b'] AS x RETURN x"));

        var e = assertThrows(NoSuchRecordException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), startsWith("Expected a result with a single record"));
    }

    @Test
    void shouldReturnSingleWithSingleRecordCursor() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("RETURN 'Hello!'"));

        var record = await(cursor.singleAsync());

        assertEquals("Hello!", record.get(0).asString());
    }

    @Test
    void shouldPropagateFailureFromFirstRecordInSingleAsync() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("UNWIND [0] AS x RETURN 10 / x"));

        var e = assertThrows(ClientException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), containsString("/ by zero"));
    }

    @Test
    void shouldNotPropagateFailureFromSecondRecordInSingleAsync() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("UNWIND [1, 0] AS x RETURN 10 / x"));

        var e = assertThrows(ClientException.class, () -> await(cursor.singleAsync()));
        assertThat(e.getMessage(), containsString("/ by zero"));
    }

    @Test
    void shouldConsumeEmptyCursor() {
        testConsume("MATCH (n:NoSuchLabel) RETURN n");
    }

    @Test
    void shouldConsumeNonEmptyCursor() {
        testConsume("RETURN 42");
    }

    @Test
    void shouldFailToRunQueryAfterCommit() {
        var tx = await(session.beginTransactionAsync());
        tx.runAsync("CREATE (:MyLabel)");
        assertNull(await(tx.commitAsync()));

        var cursor = await(session.runAsync("MATCH (n:MyLabel) RETURN count(n)"));
        assertEquals(1, await(cursor.singleAsync()).get(0).asInt());

        var e = assertThrows(ClientException.class, () -> await(tx.runAsync("CREATE (:MyOtherLabel)")));
        assertEquals("Cannot run more queries in this transaction, it has been committed", e.getMessage());
    }

    @Test
    void shouldFailToRunQueryAfterRollback() {
        var tx = await(session.beginTransactionAsync());
        tx.runAsync("CREATE (:MyLabel)");
        assertNull(await(tx.rollbackAsync()));

        var cursor = await(session.runAsync("MATCH (n:MyLabel) RETURN count(n)"));
        assertEquals(0, await(cursor.singleAsync()).get(0).asInt());

        var e = assertThrows(ClientException.class, () -> await(tx.runAsync("CREATE (:MyOtherLabel)")));
        assertEquals("Cannot run more queries in this transaction, it has been rolled back", e.getMessage());
    }

    @Test
    void shouldUpdateSessionBookmarkAfterCommit() {
        var bookmarksBefore = session.lastBookmarks();

        await(session.beginTransactionAsync()
                .thenCompose(tx -> tx.runAsync("CREATE (:MyNode)").thenCompose(ignore -> tx.commitAsync())));

        var bookmarksAfter = session.lastBookmarks();

        assertNotNull(bookmarksAfter);
        assertNotEquals(bookmarksBefore, bookmarksAfter);
    }

    @Test
    void shouldFailToCommitWhenQueriesFail() {
        var tx = await(session.beginTransactionAsync());

        await(tx.runAsync("CREATE (:TestNode)"));
        await(tx.runAsync("CREATE (:TestNode)"));
        await(tx.runAsync("RETURN 1 * \"x\"").exceptionally(ignored -> null));
        assertThrows(TransactionTerminatedException.class, () -> await(tx.runAsync("CREATE (:TestNode)")));

        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertEquals(
                "Transaction can't be committed. It has been rolled back either because of an error or explicit termination",
                e.getMessage());
    }

    @Test
    void shouldFailToCommitWhenRunFailed() {
        var tx = await(session.beginTransactionAsync());

        await(tx.runAsync("RETURN ILLEGAL").exceptionally(ignored -> null));

        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertThat(e.getMessage(), containsString("Transaction can't be committed"));
    }

    @Test
    void shouldFailToCommitWhenBlockedRunFailed() {
        var tx = await(session.beginTransactionAsync());

        var runException = assertThrows(ClientException.class, () -> await(tx.runAsync("RETURN 1 * \"x\"")));

        var commitException = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertThat(runException.getMessage(), containsString("Type mismatch"));
        assertNoCircularReferences(commitException);
        assertThat(commitException.getMessage(), containsString("Transaction can't be committed"));
    }

    @Test
    void shouldRollbackSuccessfullyWhenRunFailed() {
        var tx = await(session.beginTransactionAsync());

        await(tx.runAsync("RETURN ILLEGAL").exceptionally(ignored -> null));

        await(tx.rollbackAsync());
    }

    @Test
    void shouldRollbackSuccessfullyWhenBlockedRunFailed() {
        var tx = await(session.beginTransactionAsync());

        assertThrows(ClientException.class, () -> await(tx.runAsync("RETURN 1 * \"x\"")));

        await(tx.rollbackAsync());
    }

    @Test
    void shouldPropagatePullAllFailureFromCommit() {
        var tx = await(session.beginTransactionAsync());

        await(tx.runAsync("UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x"));

        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertThat(e.code(), containsString("TypeError"));
    }

    @Test
    void shouldPropagateBlockedPullAllFailureFromCommit() {
        var tx = await(session.beginTransactionAsync());

        await(tx.runAsync("UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x"));

        var e = assertThrows(ClientException.class, () -> await(tx.commitAsync()));
        assertNoCircularReferences(e);
        assertThat(e.code(), containsString("TypeError"));
    }

    @Test
    void shouldPropagatePullAllFailureFromRollback() {
        var tx = await(session.beginTransactionAsync());

        await(tx.runAsync("UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x"));

        var e = assertThrows(ClientException.class, () -> await(tx.rollbackAsync()));
        assertThat(e.code(), containsString("TypeError"));
    }

    @Test
    void shouldPropagateBlockedPullAllFailureFromRollback() {
        var tx = await(session.beginTransactionAsync());

        await(tx.runAsync("UNWIND [1, 2, 3, 'Hi'] AS x RETURN 10 / x"));

        var e = assertThrows(ClientException.class, () -> await(tx.rollbackAsync()));
        assertThat(e.code(), containsString("TypeError"));
    }

    @Test
    void shouldRollbackWhenPullAllFailureIsConsumed() {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync("UNWIND [1, 0] AS x RETURN 5 / x"));

        var e = assertThrows(ClientException.class, () -> await(cursor.consumeAsync()));
        assertThat(e.getMessage(), containsString("/ by zero"));
        assertNull(await(tx.rollbackAsync()));
    }

    private int countNodes(Object id) {
        var cursor = await(session.runAsync("MATCH (n:Node {id: $id}) RETURN count(n)", parameters("id", id)));
        return await(cursor.singleAsync()).get(0).asInt();
    }

    private void testForEach(String query, int expectedSeenRecords) {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync(query));

        var recordsSeen = new AtomicInteger();
        var forEachDone = cursor.forEachAsync(record -> recordsSeen.incrementAndGet());
        var summary = await(forEachDone);

        assertNotNull(summary);
        assertEquals(query, summary.query().text());
        assertEquals(emptyMap(), summary.query().parameters().asMap());
        assertEquals(expectedSeenRecords, recordsSeen.get());
    }

    private <T> void testList(String query, List<T> expectedList) {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync(query));
        var records = await(cursor.listAsync());
        var actualList =
                records.stream().map(record -> record.get(0).asObject()).collect(Collectors.toList());
        assertEquals(expectedList, actualList);
    }

    private void testConsume(String query) {
        var tx = await(session.beginTransactionAsync());
        var cursor = await(tx.runAsync(query));
        var summary = await(cursor.consumeAsync());

        assertNotNull(summary);
        assertEquals(query, summary.query().text());
        assertEquals(emptyMap(), summary.query().parameters().asMap());

        // no records should be available, they should all be consumed
        assertThrows(ResultConsumedException.class, () -> await(cursor.nextAsync()));
    }
}
