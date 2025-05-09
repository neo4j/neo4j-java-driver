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
package org.neo4j.driver.internal.adaptedbolt;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.AuthTokens;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.bolt.connection.BoltConnectionProvider;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.DatabaseName;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.RoutingContext;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.BoltValueFactory;

public class AdaptingDriverBoltConnectionProvider implements DriverBoltConnectionProvider {
    private final BoltConnectionProvider delegate;
    private final ErrorMapper errorMapper;
    private final BoltValueFactory boltValueFactory;
    private final boolean routed;
    private final BoltServerAddress address;
    private final RoutingContext routingContext;
    private final BoltAgent boltAgent;
    private final String userAgent;
    private final int connectTimeoutMillis;

    public AdaptingDriverBoltConnectionProvider(
            BoltConnectionProvider delegate,
            ErrorMapper errorMapper,
            BoltValueFactory boltValueFactory,
            boolean routed,
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis) {
        this.delegate = Objects.requireNonNull(delegate);
        this.errorMapper = Objects.requireNonNull(errorMapper);
        this.boltValueFactory = Objects.requireNonNull(boltValueFactory);
        this.routed = routed;
        this.address = Objects.requireNonNull(address);
        this.routingContext = Objects.requireNonNull(routingContext);
        this.boltAgent = Objects.requireNonNull(boltAgent);
        this.userAgent = Objects.requireNonNull(userAgent);
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    @Override
    public CompletionStage<DriverBoltConnection> connect(
            SecurityPlan securityPlan,
            DatabaseName databaseName,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
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
                        () -> authMapStageSupplier
                                .get()
                                .thenApply(map -> AuthTokens.custom(boltValueFactory.toBoltMap(map))),
                        mode,
                        bookmarks,
                        impersonatedUser,
                        minVersion,
                        notificationConfig,
                        databaseNameConsumer,
                        additionalParameters)
                .exceptionally(errorMapper::mapAndThrow)
                .thenApply(boltConnection -> new AdaptingDriverBoltConnection(
                        boltConnection,
                        routed ? new RoutedErrorMapper(boltConnection.serverAddress(), mode) : errorMapper,
                        boltValueFactory));
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return delegate.verifyConnectivity(
                        address,
                        routingContext,
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        securityPlan,
                        AuthTokens.custom(boltValueFactory.toBoltMap(authMap)))
                .exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return delegate.supportsMultiDb(
                        address,
                        routingContext,
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        securityPlan,
                        AuthTokens.custom(boltValueFactory.toBoltMap(authMap)))
                .exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return delegate.supportsSessionAuth(
                        address,
                        routingContext,
                        boltAgent,
                        userAgent,
                        connectTimeoutMillis,
                        securityPlan,
                        AuthTokens.custom(boltValueFactory.toBoltMap(authMap)))
                .exceptionally(errorMapper::mapAndThrow);
    }

    @Override
    public CompletionStage<Void> close() {
        return delegate.close().exceptionally(errorMapper::mapAndThrow);
    }
}
