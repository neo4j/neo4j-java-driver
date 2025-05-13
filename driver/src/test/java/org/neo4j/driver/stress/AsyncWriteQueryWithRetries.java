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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.summary.ResultSummary;

public class AsyncWriteQueryWithRetries<C extends AbstractContext> extends AbstractAsyncQuery<C> {
    private final AbstractStressTestBase<C> stressTest;

    AsyncWriteQueryWithRetries(AbstractStressTestBase<C> stressTest, Driver driver, boolean useBookmark) {
        super(driver, useBookmark);
        this.stressTest = stressTest;
    }

    @Override
    public CompletionStage<Void> execute(C context) {
        var session = newSession(AccessMode.WRITE, context);

        var txStage = session.executeWriteAsync(tx -> tx.runAsync("CREATE ()").thenCompose(ResultCursor::consumeAsync));

        return txStage.thenApply(resultSummary -> processResultSummary(resultSummary, context))
                .handle((nothing, throwable) -> recordAndRethrowThrowable(throwable, context))
                .whenComplete((nothing, throwable) -> finalizeSession(session, context));
    }

    private Void processResultSummary(ResultSummary resultSummary, C context) {
        assertEquals(1, resultSummary.counters().nodesCreated());
        context.nodeCreated();
        return null;
    }

    private Void recordAndRethrowThrowable(Throwable throwable, C context) {
        if (throwable != null) {
            stressTest.handleWriteFailure(throwable, context);
            throw new RuntimeException(throwable);
        }
        return null;
    }

    private void finalizeSession(AsyncSession session, C context) {
        context.setBookmark(session.lastBookmarks());
        session.closeAsync();
    }
}
