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
package org.neo4j.driver.internal.reactive;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.TransactionNestingException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.bolt.api.TelemetryApi;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.reactive.RxTransactionWork;
import org.reactivestreams.Publisher;

@Deprecated
@SuppressWarnings("DeprecatedIsStillUsed")
public class InternalRxSession extends AbstractReactiveSession<RxTransaction> implements RxSession {
    public InternalRxSession(NetworkSession session) {
        super(session);
    }

    @Override
    protected RxTransaction createTransaction(UnmanagedTransaction unmanagedTransaction) {
        return new InternalRxTransaction(unmanagedTransaction);
    }

    @Override
    protected Publisher<Void> closeTransaction(RxTransaction transaction, boolean commit) {
        return ((InternalRxTransaction) transaction).close(commit);
    }

    @Override
    public Publisher<RxTransaction> beginTransaction(TransactionConfig config) {
        return doBeginTransaction(config, new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION));
    }

    @Override
    public <T> Publisher<T> readTransaction(RxTransactionWork<? extends Publisher<T>> work) {
        return readTransaction(work, TransactionConfig.empty());
    }

    @Override
    public <T> Publisher<T> readTransaction(RxTransactionWork<? extends Publisher<T>> work, TransactionConfig config) {
        return runTransaction(AccessMode.READ, work::execute, config);
    }

    @Override
    public <T> Publisher<T> writeTransaction(RxTransactionWork<? extends Publisher<T>> work) {
        return writeTransaction(work, TransactionConfig.empty());
    }

    @Override
    public <T> Publisher<T> writeTransaction(RxTransactionWork<? extends Publisher<T>> work, TransactionConfig config) {
        return runTransaction(AccessMode.WRITE, work::execute, config);
    }

    @Override
    public RxResult run(String query, TransactionConfig config) {
        return run(new Query(query), config);
    }

    @Override
    public RxResult run(String query, Map<String, Object> parameters, TransactionConfig config) {
        return run(new Query(query, parameters), config);
    }

    @Override
    public RxResult run(Query query) {
        return run(query, TransactionConfig.empty());
    }

    @Override
    public RxResult run(Query query, TransactionConfig config) {
        return new InternalRxResult(() -> {
            var resultCursorFuture = new CompletableFuture<RxResultCursor>();
            session.runRx(query, config, resultCursorFuture).whenComplete((cursor, completionError) -> {
                if (cursor != null) {
                    resultCursorFuture.complete(cursor);
                } else {
                    releaseConnectionBeforeReturning(resultCursorFuture, completionError);
                }
            });
            return resultCursorFuture;
        });
    }

    private <T> void releaseConnectionBeforeReturning(CompletableFuture<T> returnFuture, Throwable completionError) {
        // We failed to create a result cursor so we cannot rely on result cursor to cleanup resources.
        // Therefore we will first release the connection that might have been created in the session and then notify
        // the error.
        // The logic here shall be the same as `SessionPullResponseHandler#afterFailure`.
        // The reason we need to release connection in session is that we made `rxSession.close()` optional;
        // Otherwise, session.close shall handle everything for us.
        var error = Futures.completionExceptionCause(completionError);
        if (error instanceof TransactionNestingException) {
            returnFuture.completeExceptionally(error);
        } else {
            session.releaseConnectionAsync()
                    .whenComplete((ignored, closeError) ->
                            returnFuture.completeExceptionally(Futures.combineErrors(error, closeError)));
        }
    }

    @Override
    public Bookmark lastBookmark() {
        return InternalBookmark.from(session.lastBookmarks());
    }

    @Override
    public <T> Publisher<T> close() {
        return doClose();
    }
}
