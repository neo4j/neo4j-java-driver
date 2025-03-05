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
package org.neo4j.driver.internal.boltlistener;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthToken;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnection;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.RoutingContext;
import org.neo4j.bolt.connection.SecurityPlan;

final class ListeningBoltConnectionProvider implements BoltConnectionProvider {
    private final BoltConnectionProvider delegate;
    private final BoltConnectionListener boltConnectionListener;

    public ListeningBoltConnectionProvider(
            BoltConnectionProvider delegate, BoltConnectionListener boltConnectionListener) {
        this.delegate = Objects.requireNonNull(delegate);
        this.boltConnectionListener = Objects.requireNonNull(boltConnectionListener);
    }

    @Override
    public CompletionStage<BoltConnection> connect(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            DatabaseName databaseName,
            Supplier<CompletionStage<AuthToken>> authTokenStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            Consumer<DatabaseName> databaseNameConsumer,
            Map<String, Object> additionalParameters) {
        return delegate.connect(
                        address,
                        routingContext,
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        securityPlan,
                        databaseName,
                        authTokenStageSupplier,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer,
                        additionalParameters)
                .thenApply(boltConnection -> {
                    boltConnection = new ListeningBoltConnection(boltConnection, boltConnectionListener);
                    boltConnectionListener.onOpen(boltConnection);
                    return boltConnection;
                });
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return delegate.verifyConnectivity(
                address, routingContext, boltAgent, userAgent, connectTimeoutMillis, securityPlan, authToken);
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return delegate.supportsMultiDb(
                address, routingContext, boltAgent, userAgent, connectTimeoutMillis, securityPlan, authToken);
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            SecurityPlan securityPlan,
            AuthToken authToken) {
        return delegate.supportsSessionAuth(
                address, routingContext, boltAgent, userAgent, connectTimeoutMillis, securityPlan, authToken);
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close();
    }
}
