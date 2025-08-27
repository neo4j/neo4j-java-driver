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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnectionSource;
import org.neo4j.driver.internal.async.LeakLoggingNetworkSession;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.homedb.HomeDatabaseCache;
import org.neo4j.driver.internal.observation.DriverObservationProvider;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.util.Futures;

public class SessionFactoryImpl implements SessionFactory {
    private final BoltSecurityPlanManager securityPlanManager;
    private final DriverBoltConnectionSource connectionSource;
    private final RetryLogic retryLogic;

    @SuppressWarnings("deprecation")
    private final Logging logging;

    private final boolean leakedSessionsLoggingEnabled;
    private final long defaultFetchSize;
    private final AuthTokenManager authTokenManager;
    private final HomeDatabaseCache homeDatabaseCache;
    private final DriverObservationProvider observationProvider;

    @SuppressWarnings("deprecation")
    SessionFactoryImpl(
            BoltSecurityPlanManager securityPlanManager,
            DriverBoltConnectionSource connectionSource,
            RetryLogic retryLogic,
            Config config,
            AuthTokenManager authTokenManager,
            HomeDatabaseCache homeDatabaseCache,
            DriverObservationProvider observationProvider) {
        this.securityPlanManager = Objects.requireNonNull(securityPlanManager);
        this.connectionSource = connectionSource;
        this.leakedSessionsLoggingEnabled = config.logLeakedSessions();
        this.retryLogic = retryLogic;
        this.logging = config.logging();
        this.defaultFetchSize = config.fetchSize();
        this.authTokenManager = authTokenManager;
        this.homeDatabaseCache = Objects.requireNonNull(homeDatabaseCache);
        this.observationProvider = Objects.requireNonNull(observationProvider);
    }

    @SuppressWarnings("deprecation")
    @Override
    public NetworkSession newInstance(
            SessionConfig sessionConfig, AuthToken overrideAuthToken, boolean telemetryDisabled) {
        return createSession(
                securityPlanManager,
                connectionSource,
                retryLogic,
                parseDatabaseName(sessionConfig),
                sessionConfig.defaultAccessMode(),
                toDistinctSet(sessionConfig.bookmarks()),
                parseFetchSize(sessionConfig),
                sessionConfig.impersonatedUser().orElse(null),
                logging,
                sessionConfig.bookmarkManager().orElse(NoOpBookmarkManager.INSTANCE),
                sessionConfig.notificationConfig(),
                overrideAuthToken,
                telemetryDisabled,
                authTokenManager,
                homeDatabaseCache);
    }

    private Set<Bookmark> toDistinctSet(Iterable<Bookmark> bookmarks) {
        Set<Bookmark> set = new HashSet<>();
        if (bookmarks != null) {
            for (var bookmark : bookmarks) {
                if (bookmark != null) {
                    var values = bookmark.value();
                    set.add(bookmark);
                }
            }
        }
        return Collections.unmodifiableSet(set);
    }

    private long parseFetchSize(SessionConfig sessionConfig) {
        return sessionConfig.fetchSize().orElse(defaultFetchSize);
    }

    private DatabaseName parseDatabaseName(SessionConfig sessionConfig) {
        return sessionConfig
                .database()
                .flatMap(name -> Optional.of(DatabaseName.database(name)))
                .orElse(DatabaseName.defaultDatabase());
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        return connectionSource.verifyConnectivity().exceptionally(throwable -> {
            throwable = Futures.completionExceptionCause(throwable);
            if (throwable instanceof TimeoutException) {
                throw new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(throwable.getMessage()),
                        "N/A",
                        throwable.getMessage(),
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        throwable);
            } else {
                throw new CompletionException(throwable);
            }
        });
    }

    @Override
    public CompletionStage<Void> close() {
        return connectionSource.close();
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb() {
        return connectionSource.supportsMultiDb();
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth() {
        return connectionSource.supportsSessionAuth();
    }

    private NetworkSession createSession(
            BoltSecurityPlanManager securityPlanManager,
            DriverBoltConnectionSource connectionProvider,
            RetryLogic retryLogic,
            DatabaseName databaseName,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            long fetchSize,
            String impersonatedUser,
            @SuppressWarnings("deprecation") Logging logging,
            BookmarkManager bookmarkManager,
            @SuppressWarnings("deprecation") NotificationConfig notificationConfig,
            AuthToken authToken,
            boolean telemetryDisabled,
            AuthTokenManager authTokenManager,
            HomeDatabaseCache homeDatabaseCache) {
        Objects.requireNonNull(bookmarks, "bookmarks may not be null");
        Objects.requireNonNull(bookmarkManager, "bookmarkManager may not be null");
        return leakedSessionsLoggingEnabled
                ? new LeakLoggingNetworkSession(
                        connectionProvider,
                        retryLogic,
                        databaseName,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        fetchSize,
                        logging,
                        bookmarkManager,
                        notificationConfig,
                        authToken,
                        telemetryDisabled,
                        authTokenManager,
                        homeDatabaseCache,
                        observationProvider)
                : new NetworkSession(
                        connectionProvider,
                        retryLogic,
                        databaseName,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        fetchSize,
                        logging,
                        bookmarkManager,
                        notificationConfig,
                        authToken,
                        telemetryDisabled,
                        authTokenManager,
                        homeDatabaseCache,
                        observationProvider);
    }

    public DriverBoltConnectionSource getConnectionSource() {
        return connectionSource;
    }

    private record SecurityPlanAndAuthToken(SecurityPlan securityPlan, Map<String, Value> authToken) {}
}
