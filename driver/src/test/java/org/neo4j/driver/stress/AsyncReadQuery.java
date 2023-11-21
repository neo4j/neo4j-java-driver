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

public class AsyncReadQuery<C extends AbstractContext> extends AbstractAsyncQuery<C> {
    public AsyncReadQuery(Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
    }

    @Override
    public CompletionStage<Void> execute(C context) {
        var session = newSession(AccessMode.READ, context);

        var queryFinished = session.runAsync("MATCH (n) RETURN n LIMIT 1")
                .thenCompose(cursor -> cursor.nextAsync().thenCompose(record -> processAndGetSummary(record, cursor)));

        queryFinished.whenComplete((summary, error) -> {
            if (summary != null) {
                context.readCompleted();
            }
            session.closeAsync();
        });

        return queryFinished.thenApply(summary -> null);
    }

    private CompletionStage<ResultSummary> processAndGetSummary(Record record, ResultCursor cursor) {
        if (record != null) {
            var node = record.get(0).asNode();
            assertNotNull(node);
        }
        return cursor.consumeAsync();
    }
}
