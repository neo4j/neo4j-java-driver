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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Driver;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;

@ParallelizableIT
class QueryRunnerCloseIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;
    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("resource")
    void shouldErrorToAccessRecordsAfterConsume() {
        // Given
        var result = neo4j.driver().session().run("UNWIND [1,2] AS a RETURN a");

        // When
        result.consume();

        // Then
        assertThrows(ResultConsumedException.class, result::hasNext);
        assertThrows(ResultConsumedException.class, result::next);
        assertThrows(ResultConsumedException.class, result::list);
        assertThrows(ResultConsumedException.class, result::single);
        assertThrows(ResultConsumedException.class, result::peek);
        assertThrows(ResultConsumedException.class, () -> result.stream().toArray());
        assertThrows(ResultConsumedException.class, () -> result.forEachRemaining(record -> {}));
        assertThrows(ResultConsumedException.class, () -> result.list(Function.identity()));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldErrorToAccessRecordsAfterClose() {
        // Given
        var session = neo4j.driver().session();
        var result = session.run("UNWIND [1,2] AS a RETURN a");

        // When
        session.close();

        // Then
        assertThrows(ResultConsumedException.class, result::hasNext);
        assertThrows(ResultConsumedException.class, result::next);
        assertThrows(ResultConsumedException.class, result::list);
        assertThrows(ResultConsumedException.class, result::single);
        assertThrows(ResultConsumedException.class, result::peek);
        assertThrows(ResultConsumedException.class, () -> result.stream().toArray());
        assertThrows(ResultConsumedException.class, () -> result.forEachRemaining(record -> {}));
        assertThrows(ResultConsumedException.class, () -> result.list(Function.identity()));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowConsumeAndKeysAfterConsume() {
        // Given
        var result = neo4j.driver().session().run("UNWIND [1,2] AS a RETURN a");
        var keys = result.keys();

        // When
        var summary = result.consume();

        // Then
        var summary1 = result.consume();
        var keys1 = result.keys();

        assertEquals(summary, summary1);
        assertEquals(keys, keys1);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowSummaryAndKeysAfterClose() {
        // Given
        var session = neo4j.driver().session();
        var result = session.run("UNWIND [1,2] AS a RETURN a");
        var keys = result.keys();
        var summary = result.consume();

        // When
        session.close();

        // Then
        var summary1 = result.consume();
        var keys1 = result.keys();

        assertEquals(summary, summary1);
        assertEquals(keys, keys1);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldErrorToAccessRecordsAfterConsumeAsync() {
        // Given
        var session = neo4j.driver().session(AsyncSession.class);
        var result = await(session.runAsync("UNWIND [1,2] AS a RETURN a"));

        // When
        await(result.consumeAsync());

        // Then
        assertThrows(ResultConsumedException.class, () -> await(result.nextAsync()));
        assertThrows(ResultConsumedException.class, () -> await(result.peekAsync()));
        assertThrows(ResultConsumedException.class, () -> await(result.singleAsync()));
        assertThrows(ResultConsumedException.class, () -> await(result.forEachAsync(record -> {})));
        assertThrows(ResultConsumedException.class, () -> await(result.listAsync()));
        assertThrows(ResultConsumedException.class, () -> await(result.listAsync(Function.identity())));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldErrorToAccessRecordsAfterCloseAsync() {
        // Given
        var session = neo4j.driver().session(AsyncSession.class);
        var result = await(session.runAsync("UNWIND [1,2] AS a RETURN a"));

        // When
        await(session.closeAsync());

        // Then
        assertThrows(ResultConsumedException.class, () -> await(result.nextAsync()));
        assertThrows(ResultConsumedException.class, () -> await(result.peekAsync()));
        assertThrows(ResultConsumedException.class, () -> await(result.singleAsync()));
        assertThrows(ResultConsumedException.class, () -> await(result.forEachAsync(record -> {})));
        assertThrows(ResultConsumedException.class, () -> await(result.listAsync()));
        assertThrows(ResultConsumedException.class, () -> await(result.listAsync(Function.identity())));
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowConsumeAndKeysAfterConsumeAsync() {
        // Given
        var session = neo4j.driver().session(AsyncSession.class);
        var result = await(session.runAsync("UNWIND [1,2] AS a RETURN a"));

        var keys = result.keys();

        // When
        var summary = await(result.consumeAsync());

        // Then
        var summary1 = await(result.consumeAsync());
        var keys1 = result.keys();

        assertEquals(summary, summary1);
        assertEquals(keys, keys1);
    }

    @Test
    @SuppressWarnings("resource")
    void shouldAllowConsumeAndKeysAfterCloseAsync() {
        // Given
        var session = neo4j.driver().session(AsyncSession.class);
        var result = await(session.runAsync("UNWIND [1,2] AS a RETURN a"));
        var keys = result.keys();
        var summary = await(result.consumeAsync());

        // When
        await(session.closeAsync());

        // Then
        var keys1 = result.keys();
        var summary1 = await(result.consumeAsync());

        assertEquals(summary, summary1);
        assertEquals(keys, keys1);
    }
}
