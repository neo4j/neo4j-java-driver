/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import static org.neo4j.driver.TransactionClusterMemberAccess.READERS;
import static org.neo4j.driver.TransactionClusterMemberAccess.WRITERS;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.Driver;
import org.neo4j.driver.DriverQueryConfig;
import org.neo4j.driver.DriverTransactionConfig;
import org.neo4j.driver.EagerQueryResult;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.async.InternalAsyncSession;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.metrics.DevNullMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.reactive.InternalReactiveSession;
import org.neo4j.driver.internal.reactive.InternalRxSession;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.types.TypeSystem;

public class InternalDriver implements Driver {
    private final SecurityPlan securityPlan;
    private final SessionFactory sessionFactory;
    private final Logger log;

    private AtomicBoolean closed = new AtomicBoolean(false);
    private final MetricsProvider metricsProvider;

    private final BookmarkManager bookmarkManager;

    InternalDriver(
            SecurityPlan securityPlan,
            SessionFactory sessionFactory,
            MetricsProvider metricsProvider,
            Logging logging,
            BookmarkManager bookmarkManager) {
        this.securityPlan = securityPlan;
        this.sessionFactory = sessionFactory;
        this.metricsProvider = metricsProvider;
        this.log = logging.getLog(getClass());
        this.bookmarkManager = bookmarkManager != null ? bookmarkManager : new NoOpBookmarkManager();
    }

    @Override
    public Session session() {
        return new InternalSession(newSession(SessionConfig.defaultConfig()));
    }

    @Override
    public Session session(SessionConfig sessionConfig) {
        return new InternalSession(newSession(sessionConfig));
    }

    @Override
    @Deprecated
    public RxSession rxSession(SessionConfig sessionConfig) {
        return new InternalRxSession(newSession(sessionConfig));
    }

    @Override
    public ReactiveSession reactiveSession(SessionConfig sessionConfig) {
        return new InternalReactiveSession(newSession(sessionConfig));
    }

    @Override
    public AsyncSession asyncSession() {
        return new InternalAsyncSession(newSession(SessionConfig.defaultConfig()));
    }

    @Override
    public AsyncSession asyncSession(SessionConfig sessionConfig) {
        return new InternalAsyncSession(newSession(sessionConfig));
    }

    @Override
    public Metrics metrics() {
        return metricsProvider.metrics();
    }

    @Override
    public boolean isMetricsEnabled() {
        return metricsProvider != DevNullMetricsProvider.INSTANCE;
    }

    @Override
    public boolean isEncrypted() {
        assertOpen();
        return securityPlan.requiresEncryption();
    }

    @Override
    public EagerQueryResult query(Query query, DriverQueryConfig config) {
        var sessionConfigBuilder = SessionConfig.builder().withBookmarks(config.bookmarks());
        config.database().ifPresent(sessionConfigBuilder::withDatabase);
        config.impersonatedUser().ifPresent(sessionConfigBuilder::withImpersonatedUser);
        var accessMode =
                switch (config.clusterMemberAccess()) {
                    case AUTO, WRITERS -> AccessMode.WRITE;
                    case READERS -> AccessMode.READ;
                };
        sessionConfigBuilder.withDefaultAccessMode(accessMode);
        try (var session = session(sessionConfigBuilder.build())) {
            var transactionConfigBuilder = TransactionConfig.builder();
            config.timeout().ifPresent(transactionConfigBuilder::withTimeout);
            transactionConfigBuilder.withMetadata(config.metadata());
            if (config.explicitTransaction()) {
                var memberAccess =
                        switch (config.clusterMemberAccess()) {
                            case AUTO, WRITERS -> WRITERS;
                            case READERS -> READERS;
                        };
                return ((InternalSession) session)
                        .execute(
                                tx -> initEagerResult(tx.run(query), config),
                                transactionConfigBuilder.build(),
                                memberAccess,
                                config.retryStrategy());
            } else {
                var result = session.run(query, transactionConfigBuilder.build());
                return initEagerResult(result, config);
            }
        }
    }

    @Override
    public <T> T execute(TransactionCallback<T> callback, DriverTransactionConfig config) {
        var sessionConfigBuilder = SessionConfig.builder().withBookmarks(config.bookmarks());
        config.database().ifPresent(sessionConfigBuilder::withDatabase);
        config.impersonatedUser().ifPresent(sessionConfigBuilder::withImpersonatedUser);
        var transactionConfigBuilder = TransactionConfig.builder();
        config.timeout().ifPresent(transactionConfigBuilder::withTimeout);
        transactionConfigBuilder.withMetadata(config.metadata());
        try (var session = session(sessionConfigBuilder.build())) {
            return ((InternalSession) session)
                    .execute(
                            callback,
                            transactionConfigBuilder.build(),
                            config.clusterMemberAccess(),
                            config.retryStrategy());
        }
    }

    private InternalEagerQueryResult initEagerResult(Result result, DriverQueryConfig config) {
        return new InternalEagerQueryResult(result.keys(), listRecords(result, config), result.consume());
    }

    private List<Record> listRecords(Result result, DriverQueryConfig config) {
        List<Record> records = new ArrayList<>();
        if (!config.skipRecords()) {
            long countdownLimit = config.maxRecordCount() + 1;
            while (result.hasNext()) {
                countdownLimit--;
                if (countdownLimit == 0) {
                    // todo determine exception
                    throw new ClientException("Maximum record set size exceeded");
                }
                records.add(result.next());
            }
        }
        return records;
    }

    @Override
    public void close() {
        Futures.blockingGet(closeAsync());
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing driver instance %s", hashCode());
            return sessionFactory.close();
        }
        return completedWithNull();
    }

    @Override
    public final TypeSystem defaultTypeSystem() {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    @Override
    public CompletionStage<Void> verifyConnectivityAsync() {
        return sessionFactory.verifyConnectivity();
    }

    @Override
    public boolean supportsMultiDb() {
        return Futures.blockingGet(supportsMultiDbAsync());
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDbAsync() {
        return sessionFactory.supportsMultiDb();
    }

    @Override
    public void verifyConnectivity() {
        Futures.blockingGet(verifyConnectivityAsync());
    }

    /**
     * Get the underlying session factory.
     * <p>
     * <b>This method is only for testing</b>
     *
     * @return the session factory used by this driver.
     */
    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    private static RuntimeException driverCloseException() {
        return new IllegalStateException("This driver instance has already been closed");
    }

    public NetworkSession newSession(SessionConfig config) {
        assertOpen();
        var bookmarkManager = config.ignoreBookmarkManager() ? new NoOpBookmarkManager() : this.bookmarkManager;
        NetworkSession session = sessionFactory.newInstance(config, bookmarkManager);
        if (closed.get()) {
            // session does not immediately acquire connection, it is fine to just throw
            throw driverCloseException();
        }
        return session;
    }

    private void assertOpen() {
        if (closed.get()) {
            throw driverCloseException();
        }
    }
}
