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
package org.neo4j.driver.integration;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V3;
import static org.neo4j.driver.testutil.TestUtil.TX_TIMEOUT_TEST_TIMEOUT;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.testutil.DriverExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@EnabledOnNeo4jWith(BOLT_V3)
@ParallelizableIT
class SessionBoltV3IT {
    @RegisterExtension
    static final DriverExtension driver = new DriverExtension();

    private static String showTxMetadata;

    @BeforeEach
    void beforeAll() {
        showTxMetadata = driver.isNeo4j43OrEarlier()
                ? "CALL dbms.listTransactions() YIELD metaData"
                : "SHOW TRANSACTIONS YIELD metaData";
    }

    @Test
    void shouldSetTransactionMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("a", "hello world");
        metadata.put("b", LocalDate.now());
        metadata.put("c", driver.isNeo4j43OrEarlier() ? asList(true, false, true) : false);

        var config = TransactionConfig.builder().withMetadata(metadata).build();

        @SuppressWarnings("resource")
        var result = driver.session().run(showTxMetadata, config);
        var receivedMetadata = result.single().get("metaData").asMap();

        assertEquals(metadata, receivedMetadata);
    }

    @Test
    void shouldSetTransactionMetadataAsync() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", 42L);

        var config = TransactionConfig.builder().withMetadata(metadata).build();

        var metadataFuture = driver.asyncSession()
                .runAsync(showTxMetadata, config)
                .thenCompose(ResultCursor::singleAsync)
                .thenApply(record -> record.get("metaData").asMap());

        assertEquals(metadata, await(metadataFuture));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldSetTransactionTimeout() {
        // create a dummy node
        var session = driver.session();
        session.run("CREATE (:Node)").consume();

        try (var otherSession = driver.driver().session()) {
            try (var otherTx = otherSession.beginTransaction()) {
                // lock dummy node but keep the transaction open
                otherTx.run("MATCH (n:Node) SET n.prop = 1").consume();

                assertTimeoutPreemptively(TX_TIMEOUT_TEST_TIMEOUT, () -> {
                    var config =
                            TransactionConfig.builder().withTimeout(ofMillis(1)).build();

                    // run a query in an auto-commit transaction with timeout and try to update the locked dummy node
                    var error = assertThrows(Exception.class, () -> session.run("MATCH (n:Node) SET n.prop = 2", config)
                            .consume());
                    verifyValidException(error);
                });
            }
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldSetTransactionTimeoutAsync() {
        // create a dummy node
        var asyncSession = driver.asyncSession();
        await(await(asyncSession.runAsync("CREATE (:Node)")).consumeAsync());

        try (var otherSession = driver.driver().session()) {
            try (var otherTx = otherSession.beginTransaction()) {
                // lock dummy node but keep the transaction open
                otherTx.run("MATCH (n:Node) SET n.prop = 1").consume();

                assertTimeoutPreemptively(TX_TIMEOUT_TEST_TIMEOUT, () -> {
                    var config =
                            TransactionConfig.builder().withTimeout(ofMillis(1)).build();

                    // run a query in an auto-commit transaction with timeout and try to update the locked dummy node
                    var resultFuture = asyncSession
                            .runAsync("MATCH (n:Node) SET n.prop = 2", config)
                            .thenCompose(ResultCursor::consumeAsync);

                    var error = assertThrows(Exception.class, () -> await(resultFuture));
                    verifyValidException(error);
                });
            }
        }
    }

    @Test
    void shouldSetTransactionMetadataWithAsyncReadTransactionFunction() {
        testTransactionMetadataWithAsyncTransactionFunctions(true);
    }

    @Test
    void shouldSetTransactionMetadataWithAsyncWriteTransactionFunction() {
        testTransactionMetadataWithAsyncTransactionFunctions(false);
    }

    @Test
    void shouldSetTransactionMetadataWithReadTransactionFunction() {
        testTransactionMetadataWithTransactionFunctions(true);
    }

    @Test
    void shouldSetTransactionMetadataWithWriteTransactionFunction() {
        testTransactionMetadataWithTransactionFunctions(false);
    }

    @Test
    void shouldUseBookmarksForAutoCommitTransactions() {
        @SuppressWarnings("resource")
        var session = driver.session();
        var initialBookmarks = session.lastBookmarks();

        session.run("CREATE ()").consume();
        var bookmarks1 = session.lastBookmarks();
        assertNotNull(bookmarks1);
        assertNotEquals(initialBookmarks, bookmarks1);

        session.run("CREATE ()").consume();
        var bookmarks2 = session.lastBookmarks();
        assertNotNull(bookmarks2);
        assertNotEquals(initialBookmarks, bookmarks2);
        assertNotEquals(bookmarks1, bookmarks2);

        session.run("CREATE ()").consume();
        var bookmarks3 = session.lastBookmarks();
        assertNotNull(bookmarks3);
        assertNotEquals(initialBookmarks, bookmarks3);
        assertNotEquals(bookmarks1, bookmarks3);
        assertNotEquals(bookmarks2, bookmarks3);
    }

    @Test
    void shouldUseBookmarksForAutoCommitAndUnmanagedTransactions() {
        @SuppressWarnings("resource")
        var session = driver.session();
        var initialBookmarks = session.lastBookmarks();

        try (var tx = session.beginTransaction()) {
            tx.run("CREATE ()");
            tx.commit();
        }
        var bookmarks1 = session.lastBookmarks();
        assertNotNull(bookmarks1);
        assertNotEquals(initialBookmarks, bookmarks1);

        session.run("CREATE ()").consume();
        var bookmarks2 = session.lastBookmarks();
        assertNotNull(bookmarks2);
        assertNotEquals(initialBookmarks, bookmarks2);
        assertNotEquals(bookmarks1, bookmarks2);

        try (var tx = session.beginTransaction()) {
            tx.run("CREATE ()");
            tx.commit();
        }
        var bookmarks3 = session.lastBookmarks();
        assertNotNull(bookmarks3);
        assertNotEquals(initialBookmarks, bookmarks3);
        assertNotEquals(bookmarks1, bookmarks3);
        assertNotEquals(bookmarks2, bookmarks3);
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldUseBookmarksForAutoCommitTransactionsAndTransactionFunctions() {
        @SuppressWarnings("resource")
        var session = driver.session();
        var initialBookmarks = session.lastBookmarks();

        session.executeWrite(tx -> tx.run("CREATE ()").consume());
        var bookmarks1 = session.lastBookmarks();
        assertNotNull(bookmarks1);
        assertNotEquals(initialBookmarks, bookmarks1);

        session.run("CREATE ()").consume();
        var bookmarks2 = session.lastBookmarks();
        assertNotNull(bookmarks2);
        assertNotEquals(initialBookmarks, bookmarks2);
        assertNotEquals(bookmarks1, bookmarks2);

        session.executeWrite(tx -> tx.run("CREATE ()").consume());
        var bookmarks3 = session.lastBookmarks();
        assertNotNull(bookmarks3);
        assertNotEquals(initialBookmarks, bookmarks3);
        assertNotEquals(bookmarks1, bookmarks3);
        assertNotEquals(bookmarks2, bookmarks3);
    }

    //    @Test
    //    void shouldSendGoodbyeWhenClosingDriver() {
    //        var txCount = 13;
    //        var driverFactory = new MessageRecordingDriverFactory();
    //
    //        try (var otherDriver = driverFactory.newInstance(
    //                driver.uri(), driver.authTokenManager(), defaultConfig(), SecurityPlanImpl.insecure(), null,
    // null)) {
    //            List<Session> sessions = new ArrayList<>();
    //            List<Transaction> txs = new ArrayList<>();
    //            for (var i = 0; i < txCount; i++) {
    //                var session = otherDriver.session();
    //                sessions.add(session);
    //                var tx = session.beginTransaction();
    //                txs.add(tx);
    //            }
    //
    //            for (var i = 0; i < txCount; i++) {
    //                var session = sessions.get(i);
    //                var tx = txs.get(i);
    //
    //                tx.run("CREATE ()");
    //                tx.commit();
    //                session.close();
    //            }
    //        }
    //
    //        var messagesByChannel = driverFactory.getMessagesByChannel();
    //        assertEquals(txCount, messagesByChannel.size());
    //
    //        for (var messages : messagesByChannel.values()) {
    //            assertThat(messages.size(), greaterThan(2));
    //            assertThat(messages.get(0), instanceOf(HelloMessage.class)); // first message is HELLO
    //            assertThat(messages.get(messages.size() - 1), instanceOf(GoodbyeMessage.class)); // last message is
    // GOODBYE
    //        }
    //    }

    private static void testTransactionMetadataWithAsyncTransactionFunctions(boolean read) {
        var asyncSession = driver.asyncSession();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("baz", true);
        metadata.put("qux", 12345L);

        var config = TransactionConfig.builder().withMetadata(metadata).build();

        var singleFuture = read
                ? asyncSession.executeReadAsync(
                        tx -> tx.runAsync(showTxMetadata).thenCompose(ResultCursor::singleAsync), config)
                : asyncSession.executeWriteAsync(
                        tx -> tx.runAsync(showTxMetadata).thenCompose(ResultCursor::singleAsync), config);

        var metadataFuture =
                singleFuture.thenApply(record -> record.get("metaData").asMap());

        assertEquals(metadata, await(metadataFuture));
    }

    private static void testTransactionMetadataWithTransactionFunctions(boolean read) {
        @SuppressWarnings("resource")
        var session = driver.session();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("foo", "bar");
        metadata.put("baz", true);
        metadata.put("qux", 12345L);

        var config = TransactionConfig.builder().withMetadata(metadata).build();

        var single = read
                ? session.executeRead(tx -> tx.run(showTxMetadata).single(), config)
                : session.executeWrite(tx -> tx.run(showTxMetadata).single(), config);

        var receivedMetadata = single.get("metaData").asMap();

        assertEquals(metadata, receivedMetadata);
    }

    private static void verifyValidException(Exception error) {
        // Server 4.1 corrected this exception to ClientException. Testing either here for compatibility
        if (error instanceof TransientException || error instanceof ClientException) {
            assertThat(error.getMessage(), containsString("terminated"));
        } else {
            fail("Expected either a TransientException or ClientException", error);
        }
    }
}
