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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.util.Futures;

public class AsyncWrongQueryWithRetries<C extends AbstractContext> extends AbstractAsyncQuery<C> {
    public AsyncWrongQueryWithRetries(Driver driver) {
        super(driver, false);
    }

    @Override
    public CompletionStage<Void> execute(C context) {
        var session = newSession(AccessMode.READ, context);

        var recordRef = new AtomicReference<Record>();
        var throwableRef = new AtomicReference<Throwable>();

        var txStage = session.executeReadAsync(tx -> tx.runAsync("RETURN Wrong")
                .thenCompose(cursor -> cursor.nextAsync().thenCompose(record -> {
                    recordRef.set(record);
                    return cursor.consumeAsync();
                })));

        CompletionStage<Void> resultsProcessingStage = txStage.handle((resultSummary, throwable) -> {
                    throwableRef.set(throwable);
                    return null;
                })
                .thenApply(nothing -> {
                    assertNull(recordRef.get());

                    var cause = Futures.completionExceptionCause(throwableRef.get());
                    assertNotNull(cause);
                    assertThat(cause, instanceOf(ClientException.class));
                    assertThat(((Neo4jException) cause).code(), containsString("SyntaxError"));
                    return null;
                });

        return resultsProcessingStage.whenComplete((nothing, throwable) -> session.closeAsync());
    }
}
