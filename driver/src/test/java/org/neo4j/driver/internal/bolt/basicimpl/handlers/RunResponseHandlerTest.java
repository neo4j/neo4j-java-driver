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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.values;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.bolt.basicimpl.util.MetadataExtractor;

class RunResponseHandlerTest {
    @Test
    void shouldNotifyRunFutureOnSuccess() {
        var runFuture = new CompletableFuture<RunSummary>();
        var handler = newHandler(runFuture);

        assertFalse(runFuture.isDone());
        handler.onSuccess(emptyMap());

        assertTrue(runFuture.isDone() && !runFuture.isCompletedExceptionally() && !runFuture.isCancelled());
    }

    @Test
    void shouldNotifyRunFutureOnFailure() {
        var runFuture = new CompletableFuture<RunSummary>();
        var handler = newHandler(runFuture);

        assertFalse(runFuture.isDone());
        var exception = new RuntimeException();
        handler.onFailure(exception);

        assertTrue(runFuture.isCompletedExceptionally());
        var executionException = assertThrows(ExecutionException.class, runFuture::get);
        assertThat(executionException.getCause(), equalTo(exception));
    }

    @Test
    void shouldThrowOnRecord() {
        var handler = newHandler();

        assertThrows(UnsupportedOperationException.class, () -> handler.onRecord(values("a", "b", "c")));
    }

    private static RunResponseHandler newHandler() {
        return newHandler(BoltProtocolV3.METADATA_EXTRACTOR);
    }

    private static RunResponseHandler newHandler(CompletableFuture<RunSummary> runFuture) {
        return newHandler(runFuture, BoltProtocolV3.METADATA_EXTRACTOR);
    }

    private static RunResponseHandler newHandler(
            @SuppressWarnings("SameParameterValue") MetadataExtractor metadataExtractor) {
        return newHandler(new CompletableFuture<>(), metadataExtractor);
    }

    private static RunResponseHandler newHandler(
            CompletableFuture<RunSummary> runFuture, MetadataExtractor metadataExtractor) {
        return new RunResponseHandler(runFuture, metadataExtractor);
    }
}
