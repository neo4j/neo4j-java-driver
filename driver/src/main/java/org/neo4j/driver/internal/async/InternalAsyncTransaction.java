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
package org.neo4j.driver.internal.async;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeAsync;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Query;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.observation.Observation;

public class InternalAsyncTransaction extends AsyncAbstractQueryRunner implements AsyncTransaction {
    private final UnmanagedTransaction tx;
    private final DriverObservationProvider observationProvider;

    public InternalAsyncTransaction(
            UnmanagedTransaction tx, DriverObservationProvider observationProvider, Observation parentObservation) {
        this.tx = tx;
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public CompletionStage<Void> commitAsync() {
        var commitObservation = observationProvider.transactionCommit(AsyncTransaction.class);
        return observeAsync(commitObservation, () -> tx.commitAsync(commitObservation));
    }

    @Override
    public CompletionStage<Void> rollbackAsync() {
        var rollbackObservation = observationProvider.transactionRollback(AsyncTransaction.class);
        return observeAsync(rollbackObservation, () -> tx.rollbackAsync(rollbackObservation));
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        var closeObservation = observationProvider.transactionClose(AsyncTransaction.class);
        return observeAsync(closeObservation, () -> tx.closeAsync(closeObservation));
    }

    @Override
    public CompletionStage<Boolean> isOpenAsync() {
        return CompletableFuture.completedFuture(isOpen());
    }

    @Override
    public CompletionStage<ResultCursor> runAsync(Query query) {
        var runObservation =
                observationProvider.transactionRun(AsyncTransaction.class, query.text(), query.parameters());
        return observeAsync(runObservation, () -> tx.runAsync(query, runObservation, ResultCursor.class));
    }

    public boolean isOpen() {
        return tx.isOpen();
    }
}
