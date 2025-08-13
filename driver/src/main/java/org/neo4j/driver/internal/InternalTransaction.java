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
package org.neo4j.driver.internal;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observe;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.observation.Observation;
import org.neo4j.driver.internal.util.Futures;

public class InternalTransaction extends AbstractQueryRunner implements Transaction {
    private final UnmanagedTransaction tx;
    private final DriverObservationProvider observationProvider;
    private final Observation parentObservation;

    public InternalTransaction(
            UnmanagedTransaction tx, DriverObservationProvider observationProvider, Observation parentObservation) {
        this.tx = tx;
        this.observationProvider = Objects.requireNonNull(observationProvider);
        this.parentObservation = parentObservation;
    }

    @Override
    public void commit() {
        observeWithParentOrSupplier(
                parentObservation,
                () -> observationProvider.transactionCommit(Transaction.class),
                (observation) -> Futures.blockingGet(
                        tx.commitAsync(observation),
                        () -> terminateConnectionOnThreadInterrupt(
                                "Thread interrupted while committing the transaction")));
    }

    @Override
    public void rollback() {
        observeWithParentOrSupplier(
                parentObservation,
                () -> observationProvider.transactionRollback(Transaction.class),
                (observation) -> Futures.blockingGet(
                        tx.rollbackAsync(observation),
                        () -> terminateConnectionOnThreadInterrupt(
                                "Thread interrupted while rolling back the transaction")));
    }

    @Override
    public void close() {
        observeWithParentOrSupplier(
                parentObservation,
                () -> observationProvider.transactionClose(Transaction.class),
                (observation) -> Futures.blockingGet(
                        tx.closeAsync(observation),
                        () -> terminateConnectionOnThreadInterrupt(
                                "Thread interrupted while closing the transaction")));
    }

    @Override
    public Result run(Query query) {
        var runObservation = observationProvider.transactionRun(Transaction.class, query.text(), query.parameters());
        return observe(runObservation, () -> {
            var cursor = Futures.blockingGet(
                    tx.runAsync(query, runObservation, Result.class),
                    () -> terminateConnectionOnThreadInterrupt(
                            "Thread interrupted while running query in transaction"));
            return new InternalResult(null, cursor);
        });
    }

    @Override
    public boolean isOpen() {
        return tx.isOpen();
    }

    /**
     * <b>THIS IS A PRIVATE API</b>
     * <p>
     * Terminates the transaction by sending the Bolt {@code RESET} message and waiting for its response as long as the
     * transaction has not already been terminated, is not closed or closing.
     *
     * @throws org.neo4j.driver.exceptions.ClientException if the transaction is closed or is closing
     * @see org.neo4j.driver.exceptions.TransactionTerminatedException
     * @since 5.11
     */
    public void terminate() {
        Futures.blockingGet(
                tx.terminateAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while terminating the transaction"));
    }

    private void terminateConnectionOnThreadInterrupt(String reason) {
        tx.connection().forceClose(reason);
    }

    private static void observeWithParentOrSupplier(
            Observation parentObservation, Supplier<Observation> observationSupplier, Consumer<Observation> runnable) {
        if (parentObservation != null) {
            runnable.accept(parentObservation);
        } else {
            var observation = observationSupplier.get();
            observe(observation, () -> runnable.accept(observation));
        }
    }
}
