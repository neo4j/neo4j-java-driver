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
import java.util.concurrent.CompletionStage;
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
import org.neo4j.driver.internal.async.LeakLoggingNetworkSession;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.security.InternalAuthToken;

public class SessionFactoryImpl implements SessionFactory {
    private final BoltSecurityPlanManager securityPlanManager;
    private final BoltConnectionProvider connectionProvider;
    private final RetryLogic retryLogic;
    private final Logging logging;
    private final boolean leakedSessionsLoggingEnabled;
    private final long defaultFetchSize;
    private final AuthTokenManager authTokenManager;

    SessionFactoryImpl(
            BoltSecurityPlanManager securityPlanManager,
            BoltConnectionProvider connectionProvider,
            RetryLogic retryLogic,
            Config config,
            AuthTokenManager authTokenManager) {
        this.securityPlanManager = Objects.requireNonNull(securityPlanManager);
        this.connectionProvider = connectionProvider;
        this.leakedSessionsLoggingEnabled = config.logLeakedSessions();
        this.retryLogic = retryLogic;
        this.logging = config.logging();
        this.defaultFetchSize = config.fetchSize();
        this.authTokenManager = authTokenManager;
    }

    @Override
    public NetworkSession newInstance(
            SessionConfig sessionConfig,
            NotificationConfig notificationConfig,
            AuthToken overrideAuthToken,
            boolean telemetryDisabled) {
        return createSession(
                securityPlanManager,
                connectionProvider,
                retryLogic,
                parseDatabaseName(sessionConfig),
                sessionConfig.defaultAccessMode(),
                toDistinctSet(sessionConfig.bookmarks()),
                parseFetchSize(sessionConfig),
                sessionConfig.impersonatedUser().orElse(null),
                logging,
                sessionConfig.bookmarkManager().orElse(NoOpBookmarkManager.INSTANCE),
                notificationConfig,
                sessionConfig.notificationConfig(),
                overrideAuthToken,
                telemetryDisabled,
                authTokenManager);
    }

    private Set<Bookmark> toDistinctSet(Iterable<Bookmark> bookmarks) {
        Set<Bookmark> set = new HashSet<>();
        if (bookmarks != null) {
            for (var bookmark : bookmarks) {
                if (bookmark != null) {
                    @SuppressWarnings("deprecation")
                    var values = bookmark.values();
                    var size = values.size();
                    if (size == 1) {
                        set.add(bookmark);
                    } else if (size > 1) {
                        for (var value : values) {
                            set.add(Bookmark.from(value));
                        }
                    }
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
                .flatMap(name -> Optional.of(DatabaseNameUtil.database(name)))
                .orElse(DatabaseNameUtil.defaultDatabase());
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        return securityPlanManager
                .plan()
                .thenCompose(securityPlan -> authTokenManager
                        .getToken()
                        .thenApply(authToken ->
                                new SecurityPlanAndAuthToken(securityPlan, ((InternalAuthToken) authToken).toMap())))
                .thenCompose(tuple -> connectionProvider.verifyConnectivity(tuple.securityPlan(), tuple.authToken()));
    }

    @Override
    public CompletionStage<Void> close() {
        return connectionProvider.close();
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb() {
        return securityPlanManager
                .plan()
                .thenCompose(securityPlan -> authTokenManager
                        .getToken()
                        .thenApply(authToken ->
                                new SecurityPlanAndAuthToken(securityPlan, ((InternalAuthToken) authToken).toMap())))
                .thenCompose(tuple -> connectionProvider.supportsMultiDb(tuple.securityPlan(), tuple.authToken()));
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth() {
        return securityPlanManager
                .plan()
                .thenCompose(securityPlan -> authTokenManager
                        .getToken()
                        .thenApply(authToken ->
                                new SecurityPlanAndAuthToken(securityPlan, ((InternalAuthToken) authToken).toMap())))
                .thenCompose(tuple -> connectionProvider.supportsSessionAuth(tuple.securityPlan(), tuple.authToken()));
    }

    private NetworkSession createSession(
            BoltSecurityPlanManager securityPlanManager,
            BoltConnectionProvider connectionProvider,
            RetryLogic retryLogic,
            DatabaseName databaseName,
            AccessMode mode,
            Set<Bookmark> bookmarks,
            long fetchSize,
            String impersonatedUser,
            Logging logging,
            BookmarkManager bookmarkManager,
            NotificationConfig driverNotificationConfig,
            NotificationConfig notificationConfig,
            AuthToken authToken,
            boolean telemetryDisabled,
            AuthTokenManager authTokenManager) {
        Objects.requireNonNull(bookmarks, "bookmarks may not be null");
        Objects.requireNonNull(bookmarkManager, "bookmarkManager may not be null");
        return leakedSessionsLoggingEnabled
                ? new LeakLoggingNetworkSession(
                        securityPlanManager,
                        connectionProvider,
                        retryLogic,
                        databaseName,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        fetchSize,
                        logging,
                        bookmarkManager,
                        driverNotificationConfig,
                        notificationConfig,
                        authToken,
                        telemetryDisabled,
                        authTokenManager)
                : new NetworkSession(
                        securityPlanManager,
                        connectionProvider,
                        retryLogic,
                        databaseName,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        fetchSize,
                        logging,
                        bookmarkManager,
                        driverNotificationConfig,
                        notificationConfig,
                        authToken,
                        telemetryDisabled,
                        authTokenManager);
    }

    public BoltConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    private record SecurityPlanAndAuthToken(SecurityPlan securityPlan, Map<String, Value> authToken) {}
}
