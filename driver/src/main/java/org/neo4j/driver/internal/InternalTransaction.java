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

import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.util.Futures;

public class InternalTransaction extends AbstractQueryRunner implements Transaction {
    private final UnmanagedTransaction tx;

    public InternalTransaction(UnmanagedTransaction tx) {
        this.tx = tx;
    }

    @Override
    public void commit() {
        Futures.blockingGet(
                tx.commitAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while committing the transaction"));
    }

    @Override
    public void rollback() {
        Futures.blockingGet(
                tx.rollbackAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while rolling back the transaction"));
    }

    @Override
    public void close() {
        Futures.blockingGet(
                tx.closeAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while closing the transaction"));
    }

    @Override
    public Result run(Query query) {
        var cursor = Futures.blockingGet(
                tx.runAsync(query),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while running query in transaction"));
        return new InternalResult(null, cursor);
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
     * @since 5.11
     * @throws org.neo4j.driver.exceptions.ClientException if the transaction is closed or is closing
     * @see org.neo4j.driver.exceptions.TransactionTerminatedException
     */
    public void terminate() {
        Futures.blockingGet(
                tx.terminateAsync(),
                () -> terminateConnectionOnThreadInterrupt("Thread interrupted while terminating the transaction"));
    }

    private void terminateConnectionOnThreadInterrupt(String reason) {
        tx.connection().forceClose(reason);
    }
}
