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
package org.neo4j.driver.observation.micrometer;

import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.ObservationRegistry;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.BaseSession;
import org.neo4j.driver.internal.observation.BoltExchangeObservation;
import org.neo4j.driver.internal.observation.BoltHandleObservation;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.observation.HttpExchangeObservation;
import org.neo4j.driver.internal.observation.Observation;
import org.neo4j.driver.types.MapAccessor;

final class DriverMicrometerObservationProvider implements MicrometerObservationProvider, DriverObservationProvider {
    private final ObservationRegistry observationRegistry;
    private final DefaultSessionRunConvention defaultSessionRunConvention;
    private final DefaultTransactionRunConvention defaultTransactionRunConvention;
    private final DefaultHttpExchangeConvention defaultHttpExchangeConvention;

    DriverMicrometerObservationProvider(
            ObservationRegistry observationRegistry,
            boolean alwaysIncludeQuery,
            boolean includeQueryParameters,
            boolean includeUrlScheme,
            boolean includeUrlTemplate,
            Predicate<String> requestHeaderPredicate,
            Predicate<String> responseHeaderPredicate) {
        this.observationRegistry = Objects.requireNonNull(observationRegistry);
        this.defaultSessionRunConvention = new DefaultSessionRunConvention(alwaysIncludeQuery, includeQueryParameters);
        this.defaultTransactionRunConvention =
                new DefaultTransactionRunConvention(alwaysIncludeQuery, includeQueryParameters);
        this.defaultHttpExchangeConvention = new DefaultHttpExchangeConvention(
                includeUrlScheme, includeUrlTemplate, requestHeaderPredicate, responseHeaderPredicate);
    }

    @Override
    public Observation sessionRun(Class<? extends BaseSession> sessionType, String query, MapAccessor parameters) {
        return from(defaultSessionRunConvention, () -> new SessionRunContext(sessionType, query, parameters));
    }

    @Override
    public Observation beginTransaction(Class<?> transactionType) {
        return from(DefaultTransactionBeginConvention.INSTANCE, () -> new TransactionBeginContext(transactionType));
    }

    @Override
    public Observation sessionExecute(Class<? extends BaseSession> sessionType, AccessMode mode) {
        return from(DefaultSessionExecuteConvention.INSTANCE, () -> new SessionExecuteContext(sessionType, mode));
    }

    @Override
    public Observation sessionClose(Class<? extends BaseSession> sessionType) {
        return from(DefaultSessionCloseConvention.INSTANCE, () -> new SessionCloseContext(sessionType));
    }

    @Override
    public Observation transactionRun(Class<?> transactionType, String query, MapAccessor parameters) {
        return from(
                defaultTransactionRunConvention, () -> new TransactionRunContext(transactionType, query, parameters));
    }

    @Override
    public Observation transactionCommit(Class<?> transactionType) {
        return from(DefaultTransactionCommitConvention.INSTANCE, () -> new TransactionCommitContext(transactionType));
    }

    @Override
    public Observation transactionRollback(Class<?> transactionType) {
        return from(
                DefaultTransactionRollbackConvention.INSTANCE, () -> new TransactionRollbackContext(transactionType));
    }

    @Override
    public Observation transactionClose(Class<?> transactionType) {
        return from(DefaultTransactionCloseConvention.INSTANCE, () -> new TransactionCloseContext(transactionType));
    }

    @Override
    public Observation resultPeek(Class<?> resultType) {
        return from(DefaultResultPeekConvention.INSTANCE, () -> new ResultPeekContext(resultType));
    }

    @Override
    public Observation resultNext(Class<?> resultType) {
        return from(DefaultResultNextConvention.INSTANCE, () -> new ResultNextContext(resultType));
    }

    @Override
    public Observation resultSingle(Class<?> resultType) {
        return from(DefaultResultSingleConvention.INSTANCE, () -> new ResultSingleContext(resultType));
    }

    @Override
    public Observation resultList(Class<?> resultType) {
        return from(DefaultResultListConvention.INSTANCE, () -> new ResultListContext(resultType));
    }

    @Override
    public Observation resultConsume(Class<?> resultType) {
        return from(DefaultResultConsumeConvention.INSTANCE, () -> new ResultConsumeContext(resultType));
    }

    @Override
    public Observation resultRecords(Class<?> resultType) {
        return from(DefaultResultRecordsConvention.INSTANCE, () -> new ResultRecordsContext(resultType));
    }

    @Override
    public Observation connectionPoolCreate(String id, URI uri, int maxSize) {
        return from(
                DefaultConnectionPoolCreateConvention.INSTANCE,
                () -> new ConnectionPoolCreateContext(id, uri, maxSize));
    }

    @Override
    public Observation connectionPoolClose(String id, URI uri) {
        return from(DefaultConnectionPoolCloseConvention.INSTANCE, () -> new ConnectionPoolCloseContext(id, uri));
    }

    @Override
    public Observation pooledConnectionCreate(String id, URI uri) {
        return from(DefaultPooledConnectionCreateConvention.INSTANCE, () -> new PooledConnectionCreateContext(id, uri));
    }

    @Override
    public Observation pooledConnectionClose(String id, URI uri) {
        return from(DefaultPooledConnectionCloseConvention.INSTANCE, () -> new PooledConnectionCloseContext(id, uri));
    }

    @Override
    public Observation pooledConnectionAcquire(String id, URI uri) {
        return from(
                DefaultPooledConnectionAcquireConvention.INSTANCE, () -> new PooledConnectionAcquireContext(id, uri));
    }

    @Override
    public Observation pooledConnectionInUse(String id, URI uri) {
        return from(DefaultPooledConnectionInUseConvention.INSTANCE, () -> new PooledConnectionInUseContext(id, uri));
    }

    @Override
    public BoltHandleObservation boltHandle(List<String> messageTypes) {
        return new BoltHandleObservationImpl(io.micrometer.observation.Observation.createNotStarted(
                null,
                DefaultBoltHandleConvention.INSTANCE,
                () -> new BoltHandleContext(messageTypes),
                observationRegistry));
    }

    @Override
    public BoltExchangeObservation boltExchange(
            String host, int port, BoltProtocolVersion boltVersion, BiConsumer<String, String> setter) {
        return new BoltExchangeObservationImpl(io.micrometer.observation.Observation.createNotStarted(
                null,
                DefaultBoltExchangeConvention.INSTANCE,
                () -> new BoltExchangeContext(host, port, boltVersion.toString(), setter),
                observationRegistry));
    }

    @Override
    public HttpExchangeObservation httpExchange(
            URI uri, String method, String uriTemplate, BiConsumer<String, String> setter) {
        return new HttpExchangeObservationImpl(io.micrometer.observation.Observation.createNotStarted(
                null,
                defaultHttpExchangeConvention,
                () -> new HttpExchangeContext(uri, method, uriTemplate, setter),
                observationRegistry));
    }

    @Override
    public Observation scopedObservation() {
        var observation = observationRegistry.getCurrentObservation();
        return observation != null ? new ObservationImpl(observation) : null;
    }

    private <T extends io.micrometer.observation.Observation.Context> Observation from(
            ObservationConvention<T> defaultConvention, Supplier<T> contextSupplier) {
        var observation = io.micrometer.observation.Observation.createNotStarted(
                null, defaultConvention, contextSupplier, observationRegistry);
        return new ObservationImpl(observation);
    }
}
