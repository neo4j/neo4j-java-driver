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
package org.neo4j.driver.stress;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.summary.ResultSummary;

public class AsyncReadQueryWithRetries<C extends AbstractContext> extends AbstractAsyncQuery<C> {
    public AsyncReadQueryWithRetries(Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
    }

    @Override
    public CompletionStage<Void> execute(C context) {
        var session = newSession(AccessMode.READ, context);

        var txStage = session.executeReadAsync(
                tx -> tx.runAsync("MATCH (n) RETURN n LIMIT 1").thenCompose(cursor -> cursor.nextAsync()
                        .thenCompose(record -> processRecordAndGetSummary(record, cursor))));

        var resultsProcessingStage = txStage.thenApply(resultSummary -> processResultSummary(resultSummary, context));

        return resultsProcessingStage.whenComplete((nothing, throwable) -> session.closeAsync());
    }

    private CompletionStage<ResultSummary> processRecordAndGetSummary(Record record, ResultCursor cursor) {
        if (record != null) {
            var node = record.get(0).asNode();
            assertNotNull(node);
        }
        return cursor.consumeAsync();
    }

    private Void processResultSummary(ResultSummary resultSummary, C context) {
        if (resultSummary != null) {
            context.readCompleted();
        }
        return null;
    }
}
