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

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.BaseSession;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.BookmarkManagerConfig;
import org.neo4j.driver.BookmarkManagers;
import org.neo4j.driver.Driver;
import org.neo4j.driver.ExecutableQuery;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.Query;
import org.neo4j.driver.QueryConfig;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.internal.async.InternalAsyncSession;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.metrics.DevNullMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.reactive.InternalRxSession;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.types.TypeSystem;

public class InternalDriver implements Driver {
    private static final Set<String> INVALID_TOKEN_CODES = Set.of(
            "Neo.ClientError.Security.CredentialsExpired",
            "Neo.ClientError.Security.Forbidden",
            "Neo.ClientError.Security.TokenExpired",
            "Neo.ClientError.Security.Unauthorized");
    private final BookmarkManager queryBookmarkManager =
            BookmarkManagers.defaultManager(BookmarkManagerConfig.builder().build());
    private final SecurityPlan securityPlan;
    private final SessionFactory sessionFactory;
    private final Logger log;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final MetricsProvider metricsProvider;

    InternalDriver(
            SecurityPlan securityPlan,
            SessionFactory sessionFactory,
            MetricsProvider metricsProvider,
            Logging logging) {
        this.securityPlan = securityPlan;
        this.sessionFactory = sessionFactory;
        this.metricsProvider = metricsProvider;
        this.log = logging.getLog(getClass());
    }

    @Override
    public ExecutableQuery executableQuery(String query) {
        return new InternalExecutableQuery(this, new Query(query), QueryConfig.defaultConfig());
    }

    @Override
    public BookmarkManager executableQueryBookmarkManager() {
        return queryBookmarkManager;
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    @Override
    public <T extends BaseSession> T session(
            Class<T> sessionClass, SessionConfig sessionConfig, AuthToken sessionAuthToken) {
        requireNonNull(sessionClass, "sessionClass must not be null");
        requireNonNull(sessionClass, "sessionConfig must not be null");
        T session;
        if (Session.class.isAssignableFrom(sessionClass)) {
            session = (T) new InternalSession(newSession(sessionConfig, sessionAuthToken));
        } else if (AsyncSession.class.isAssignableFrom(sessionClass)) {
            session = (T) new InternalAsyncSession(newSession(sessionConfig, sessionAuthToken));
        } else if (org.neo4j.driver.reactive.ReactiveSession.class.isAssignableFrom(sessionClass)) {
            session = (T) new org.neo4j.driver.internal.reactive.InternalReactiveSession(
                    newSession(sessionConfig, sessionAuthToken));
        } else if (org.neo4j.driver.reactivestreams.ReactiveSession.class.isAssignableFrom(sessionClass)) {
            session = (T) new org.neo4j.driver.internal.reactivestreams.InternalReactiveSession(
                    newSession(sessionConfig, sessionAuthToken));
        } else if (RxSession.class.isAssignableFrom(sessionClass)) {
            session = (T) new InternalRxSession(newSession(sessionConfig, sessionAuthToken));
        } else {
            throw new IllegalArgumentException(
                    String.format("Unsupported session type '%s'", sessionClass.getCanonicalName()));
        }
        return session;
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

    @Deprecated
    @Override
    public final TypeSystem defaultTypeSystem() {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    @Override
    public CompletionStage<Void> verifyConnectivityAsync() {
        return sessionFactory.verifyConnectivity();
    }

    @Override
    public boolean verifyAuthentication(AuthToken authToken) {
        var config = SessionConfig.builder()
                .withDatabase("system")
                .withDefaultAccessMode(AccessMode.READ)
                .build();
        try (var session = session(Session.class, config, authToken)) {
            session.run("SHOW DEFAULT DATABASE").consume();
            return true;
        } catch (RuntimeException e) {
            if (e instanceof Neo4jException neo4jException) {
                if (e instanceof UnsupportedFeatureException) {
                    throw new UnsupportedFeatureException(
                            "Unable to verify authentication due to an unsupported feature", e);
                } else if (INVALID_TOKEN_CODES.contains(neo4jException.code())) {
                    return false;
                }
            }
            throw e;
        }
    }

    @Override
    public boolean supportsSessionAuth() {
        return Futures.blockingGet(sessionFactory.supportsSessionAuth());
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

    public NetworkSession newSession(SessionConfig config, AuthToken overrideAuthToken) {
        assertOpen();
        var session = sessionFactory.newInstance(config, overrideAuthToken);
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
