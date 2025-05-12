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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransactionCallback;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class SessionMixIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private AsyncSession asyncSession;
    private Session session;

    @BeforeEach
    void setUp() {
        asyncSession = newAsyncSession();
        session = newSession();
    }

    @AfterEach
    void tearDown() {
        await(asyncSession.closeAsync());
        session.close();
    }

    @SuppressWarnings("resource")
    private AsyncSession newAsyncSession() {
        return neo4j.driver().session(AsyncSession.class);
    }

    @SuppressWarnings("resource")
    private Session newSession() {
        return neo4j.driver().session();
    }

    @Test
    void shouldFailToExecuteBlockingRunChainedWithAsyncTransaction() {
        CompletionStage<Void> result = asyncSession
                .beginTransactionAsync(TransactionConfig.empty())
                .thenApply(tx -> {
                    if (Futures.isEventLoopThread(Thread.currentThread())) {
                        var e = assertThrows(IllegalStateException.class, () -> session.run("CREATE ()"));
                        assertTrue(e.getMessage().startsWith("Blocking operation can't be executed in IO thread"));
                    }
                    return null;
                });

        assertNull(await(result));
    }

    @Test
    void shouldAllowUsingBlockingApiInCommonPoolWhenChaining() {
        var txStage = asyncSession
                .beginTransactionAsync()
                // move execution to ForkJoinPool.commonPool()
                .thenApplyAsync(tx -> {
                    session.run("UNWIND [1,1,2] AS x CREATE (:Node {id: x})").consume();
                    session.run("CREATE (:Node {id: 42})").consume();
                    tx.commitAsync();
                    return tx;
                });

        await(txStage);

        assertEquals(2, countNodes(1));
        assertEquals(1, countNodes(2));
        assertEquals(1, countNodes(42));
    }

    @Test
    void shouldFailToExecuteBlockingRunInAsyncTransactionFunction() {
        AsyncTransactionCallback<CompletionStage<Void>> completionStageTransactionWork = tx -> {
            if (Futures.isEventLoopThread(Thread.currentThread())) {
                var e = assertThrows(
                        IllegalStateException.class,
                        () -> session.run("UNWIND range(1, 10000) AS x CREATE (n:AsyncNode {x: x}) RETURN n"));
                assertTrue(e.getMessage().startsWith("Blocking operation can't be executed in IO thread"));
            }
            return completedFuture(null);
        };

        var result = asyncSession.executeReadAsync(completionStageTransactionWork);
        assertNull(await(result));
    }

    @Test
    void shouldFailToExecuteBlockingRunChainedWithAsyncRun() {
        CompletionStage<Void> result = asyncSession
                .runAsync("RETURN 1")
                .thenCompose(ResultCursor::singleAsync)
                .thenApply(record -> {
                    if (Futures.isEventLoopThread(Thread.currentThread())) {
                        var e = assertThrows(
                                IllegalStateException.class,
                                () -> session.run(
                                        "RETURN $x",
                                        parameters("x", record.get(0).asInt())));
                        assertTrue(e.getMessage().startsWith("Blocking operation can't be executed in IO thread"));
                    }
                    return null;
                });

        assertNull(await(result));
    }

    @Test
    void shouldAllowBlockingOperationInCommonPoolWhenChaining() {
        var nodeStage = asyncSession
                .runAsync("RETURN 42 AS value")
                .thenCompose(ResultCursor::singleAsync)
                // move execution to ForkJoinPool.commonPool()
                .thenApplyAsync(record -> session.run("CREATE (n:Node {value: $value}) RETURN n", record))
                .thenApply(Result::single)
                .thenApply(record -> record.get(0).asNode());

        var node = await(nodeStage);

        assertEquals(42, node.get("value").asInt());
        assertEquals(1, countNodes());
    }

    private long countNodes() {
        var countStage = asyncSession
                .runAsync("MATCH (n:Node) RETURN count(n)")
                .thenCompose(ResultCursor::singleAsync)
                .thenApply(record -> record.get(0).asLong());

        return await(countStage);
    }

    private int countNodes(Object id) {
        var query = new Query("MATCH (n:Node {id: $id}) RETURN count(n)", parameters("id", id));
        var record = session.run(query).single();
        return record.get(0).asInt();
    }
}
