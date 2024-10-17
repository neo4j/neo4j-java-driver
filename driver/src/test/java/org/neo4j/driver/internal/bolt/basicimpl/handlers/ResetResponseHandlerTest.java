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
package org.neo4j.driver.internal.bolt.basicimpl.handlers;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.values;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ResetResponseHandlerTest {
    @Test
    void shouldCompleteFutureOnSuccess() throws Exception {
        var future = new CompletableFuture<Void>();
        var handler = newHandler(future);

        assertFalse(future.isDone());

        handler.onSuccess(emptyMap());

        assertTrue(future.isDone());
        assertNull(future.get());
    }

    @Test
    void shouldCompleteFutureOnFailure() {
        var future = new CompletableFuture<Void>();
        var handler = newHandler(future);

        assertFalse(future.isDone());

        handler.onFailure(new RuntimeException());

        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void shouldThrowWhenOnRecord() {
        var handler = newHandler(new CompletableFuture<>());

        assertThrows(UnsupportedOperationException.class, () -> handler.onRecord(values(1, 2, 3)));
    }

    private static ResetResponseHandler newHandler(CompletableFuture<Void> future) {
        return new ResetResponseHandler(future);
    }
}
