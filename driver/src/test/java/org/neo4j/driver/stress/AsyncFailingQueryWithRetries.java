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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.driver.internal.util.Matchers.arithmeticError;

import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.util.Futures;

public class AsyncFailingQueryWithRetries<C extends AbstractContext> extends AbstractAsyncQuery<C> {
    public AsyncFailingQueryWithRetries(Driver driver) {
        super(driver, false);
    }

    @Override
    public CompletionStage<Void> execute(C context) {
        var session = newSession(AccessMode.READ, context);

        var txStage = session.executeReadAsync(
                tx -> tx.runAsync("UNWIND [10, 5, 0] AS x RETURN 10 / x").thenCompose(ResultCursor::listAsync));

        CompletionStage<Void> resultsProcessingStage = txStage.handle((records, error) -> {
            assertNull(records);
            var cause = Futures.completionExceptionCause(error);
            assertThat(cause, is(arithmeticError()));

            return null;
        });

        return resultsProcessingStage.whenComplete((nothing, throwable) -> session.closeAsync());
    }
}
