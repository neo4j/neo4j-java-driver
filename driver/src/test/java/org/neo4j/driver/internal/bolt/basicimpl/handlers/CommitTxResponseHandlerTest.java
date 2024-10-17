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
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.testutil.TestUtil.await;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;

class CommitTxResponseHandlerTest {
    private final CompletableFuture<String> future = new CompletableFuture<>();
    private final CommitTxResponseHandler handler = new CommitTxResponseHandler(future);

    @Test
    void shouldHandleSuccessWithoutBookmark() {
        handler.onSuccess(emptyMap());

        assertNull(await(future));
    }

    @Test
    void shouldHandleSuccessWithBookmark() {
        var bookmarkString = "neo4j:bookmark:v1:tx12345";

        handler.onSuccess(singletonMap("bookmark", value(bookmarkString)));

        assertEquals(bookmarkString, await(future));
    }

    @Test
    void shouldHandleFailure() {
        var error = new RuntimeException("Hello");

        handler.onFailure(error);

        var receivedError = assertThrows(RuntimeException.class, () -> await(future));
        assertEquals(error, receivedError);
    }

    @Test
    void shouldFailToHandleRecord() {
        assertThrows(UnsupportedOperationException.class, () -> handler.onRecord(new Value[] {value(42)}));
    }
}
