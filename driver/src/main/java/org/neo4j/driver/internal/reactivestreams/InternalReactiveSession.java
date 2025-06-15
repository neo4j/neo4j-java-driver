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
package org.neo4j.driver.internal.reactivestreams;

import static org.neo4j.driver.internal.observation.util.ObservationUtil.observeStreams;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.neo4j.bolt.connection.TelemetryApi;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.observation.Observation;
import org.neo4j.driver.internal.reactive.AbstractReactiveSession;
import org.neo4j.driver.internal.telemetry.ApiTelemetryWork;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.reactivestreams.ReactiveTransaction;
import org.neo4j.driver.reactivestreams.ReactiveTransactionCallback;
import org.reactivestreams.Publisher;

public class InternalReactiveSession extends AbstractReactiveSession<ReactiveTransaction>
        implements ReactiveSession, BaseReactiveQueryRunner {
    private final DriverObservationProvider observationProvider;

    public InternalReactiveSession(NetworkSession session, DriverObservationProvider observationProvider) {
        super(session);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @Override
    public ReactiveTransaction createTransaction(UnmanagedTransaction unmanagedTransaction) {
        return new InternalReactiveTransaction(unmanagedTransaction, observationProvider);
    }

    @Override
    public Publisher<Void> closeTransaction(
            ReactiveTransaction transaction, boolean commit, Observation parentObservation) {
        return ((InternalReactiveTransaction) transaction).close(commit, parentObservation);
    }

    @Override
    public Publisher<ReactiveTransaction> beginTransaction(TransactionConfig config) {
        return beginTransaction(config, null, new ApiTelemetryWork(TelemetryApi.UNMANAGED_TRANSACTION));
    }

    public Publisher<ReactiveTransaction> beginTransaction(
            TransactionConfig config, String txType, ApiTelemetryWork apiTelemetryWork) {
        var beginObservation = observationProvider.beginTransaction(ReactiveTransaction.class);
        return observeStreams(beginObservation, doBeginTransaction(config, txType, apiTelemetryWork, beginObservation));
    }

    @Override
    public <T> Publisher<T> executeRead(
            ReactiveTransactionCallback<? extends Publisher<T>> callback, TransactionConfig config) {
        return runTransaction(
                AccessMode.READ,
                tx -> callback.execute(new DelegatingReactiveTransactionContext(tx)),
                config,
                ReactiveSession.class,
                observationProvider);
    }

    @Override
    public <T> Publisher<T> executeWrite(
            ReactiveTransactionCallback<? extends Publisher<T>> callback, TransactionConfig config) {
        return runTransaction(
                AccessMode.WRITE,
                tx -> callback.execute(new DelegatingReactiveTransactionContext(tx)),
                config,
                ReactiveSession.class,
                observationProvider);
    }

    @Override
    public Publisher<ReactiveResult> run(Query query) {
        return run(query, TransactionConfig.empty());
    }

    @Override
    public Publisher<ReactiveResult> run(Query query, TransactionConfig config) {
        var runObservation = observationProvider.sessionRun(ReactiveSession.class, query.text(), query.parameters());
        Publisher<ReactiveResult> publisher =
                run(query, config, result -> new InternalReactiveResult(result, observationProvider), runObservation);
        return observeStreams(runObservation, publisher);
    }

    @Override
    public Set<Bookmark> lastBookmarks() {
        return new HashSet<>(session.lastBookmarks());
    }

    @Override
    public <T> Publisher<T> close() {
        var closeObservation = observationProvider.sessionClose(ReactiveSession.class);
        return observeStreams(closeObservation, doClose(closeObservation));
    }
}
