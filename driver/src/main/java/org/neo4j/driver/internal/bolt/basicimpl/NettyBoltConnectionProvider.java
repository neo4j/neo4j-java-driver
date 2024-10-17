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
package org.neo4j.driver.internal.bolt.basicimpl;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;
import org.neo4j.driver.internal.bolt.api.ListenerEvent;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.MetricsListener;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.ResponseHandler;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.api.exception.MinVersionAcquisitionException;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v51.BoltProtocolV51;

public final class NettyBoltConnectionProvider implements BoltConnectionProvider {
    private final LoggingProvider logging;
    private final System.Logger log;

    private final ConnectionProvider connectionProvider;

    private BoltServerAddress address;

    private RoutingContext routingContext;
    private BoltAgent boltAgent;
    private String userAgent;
    private int connectTimeoutMillis;
    private CompletableFuture<Void> closeFuture;
    private MetricsListener metricsListener;
    private final Clock clock;

    public NettyBoltConnectionProvider(
            EventLoopGroup eventLoopGroup,
            Clock clock,
            DomainNameResolver domainNameResolver,
            LocalAddress localAddress,
            LoggingProvider logging) {
        Objects.requireNonNull(eventLoopGroup);
        this.clock = Objects.requireNonNull(clock);
        this.logging = Objects.requireNonNull(logging);
        this.log = logging.getLog(getClass());
        this.connectionProvider =
                ConnectionProviders.netty(eventLoopGroup, clock, domainNameResolver, localAddress, logging);
    }

    @Override
    public CompletionStage<Void> init(
            BoltServerAddress address,
            RoutingContext routingContext,
            BoltAgent boltAgent,
            String userAgent,
            int connectTimeoutMillis,
            MetricsListener metricsListener) {
        this.address = address;
        this.routingContext = routingContext;
        this.boltAgent = boltAgent;
        this.userAgent = userAgent;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.metricsListener = new MetricsListener() {
            @Override
            public void beforeCreating(String poolId, ListenerEvent<?> creatingEvent) {}

            @Override
            public void afterCreated(String poolId, ListenerEvent<?> creatingEvent) {}

            @Override
            public void afterFailedToCreate(String poolId) {}

            @Override
            public void afterClosed(String poolId) {}

            @Override
            public void beforeAcquiringOrCreating(String poolId, ListenerEvent<?> acquireEvent) {}

            @Override
            public void afterAcquiringOrCreating(String poolId) {}

            @Override
            public void afterAcquiredOrCreated(String poolId, ListenerEvent<?> acquireEvent) {}

            @Override
            public void afterTimedOutToAcquireOrCreate(String poolId) {}

            @Override
            public void afterConnectionCreated(String poolId, ListenerEvent<?> inUseEvent) {}

            @Override
            public void afterConnectionReleased(String poolId, ListenerEvent<?> inUseEvent) {}

            @Override
            public ListenerEvent<?> createListenerEvent() {
                return new ListenerEvent<>() {
                    @Override
                    public void start() {}

                    @Override
                    public Object getSample() {
                        return null;
                    }
                };
            }

            @Override
            public void registerPoolMetrics(
                    String poolId,
                    BoltServerAddress serverAddress,
                    IntSupplier inUseSupplier,
                    IntSupplier idleSupplier) {}

            @Override
            public void removePoolMetrics(String poolId) {}
        };
        InternalLoggerFactory.setDefaultFactory(new NettyLogging(logging));
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<BoltConnection> connect(
            SecurityPlan securityPlan,
            DatabaseName databaseName,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            AccessMode mode,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig,
            Consumer<DatabaseName> databaseNameConsumer) {
        synchronized (this) {
            if (closeFuture != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("Connection provider is closed."));
            }
        }

        var latestAuthMillisFuture = new CompletableFuture<Long>();
        var authMapRef = new AtomicReference<Map<String, Value>>();
        List<Function<ResponseHandler, CompletionStage<Void>>> messagePipeline = new ArrayList<>();
        return authMapStageSupplier
                .get()
                .thenCompose(authMap -> {
                    authMapRef.set(authMap);
                    return this.connectionProvider.acquireConnection(
                            address,
                            securityPlan,
                            routingContext,
                            databaseName != null ? databaseName.databaseName().orElse(null) : null,
                            authMap,
                            boltAgent,
                            userAgent,
                            mode,
                            connectTimeoutMillis,
                            impersonatedUser,
                            latestAuthMillisFuture,
                            notificationConfig,
                            metricsListener);
                })
                .thenCompose(connection -> {
                    if (minVersion != null
                            && minVersion.compareTo(connection.protocol().version()) > 0) {
                        return connection
                                .close()
                                .thenCompose(
                                        (ignored) -> CompletableFuture.failedStage(new MinVersionAcquisitionException(
                                                "lower version",
                                                connection.protocol().version())));
                    } else {
                        return CompletableFuture.completedStage(connection);
                    }
                })
                .handle((connection, throwable) -> {
                    if (throwable != null) {
                        log.log(System.Logger.Level.DEBUG, "Failed to establish BoltConnection " + address, throwable);
                        throw new CompletionException(throwable);
                    } else {
                        databaseNameConsumer.accept(databaseName);
                        return new BoltConnectionImpl(
                                connection.protocol(),
                                connection,
                                connection.eventLoop(),
                                authMapRef.get(),
                                latestAuthMillisFuture,
                                routingContext,
                                clock,
                                logging);
                    }
                });
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return connect(
                        securityPlan,
                        null,
                        () -> CompletableFuture.completedStage(authMap),
                        AccessMode.WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        null,
                        (ignored) -> {})
                .thenCompose(BoltConnection::close);
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return connect(
                        securityPlan,
                        null,
                        () -> CompletableFuture.completedStage(authMap),
                        AccessMode.WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        null,
                        (ignored) -> {})
                .thenCompose(boltConnection -> {
                    var supports = boltConnection.protocolVersion().compareTo(BoltProtocolV4.VERSION) >= 0;
                    return boltConnection.close().thenApply(ignored -> supports);
                });
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return connect(
                        securityPlan,
                        null,
                        () -> CompletableFuture.completedStage(authMap),
                        AccessMode.WRITE,
                        Collections.emptySet(),
                        null,
                        null,
                        null,
                        (ignored) -> {})
                .thenCompose(boltConnection -> {
                    var supports = BoltProtocolV51.VERSION.compareTo(boltConnection.protocolVersion()) <= 0;
                    return boltConnection.close().thenApply(ignored -> supports);
                });
    }

    @Override
    public CompletionStage<Void> close() {
        CompletableFuture<Void> closeFuture;
        synchronized (this) {
            if (this.closeFuture == null) {
                this.closeFuture = CompletableFuture.completedFuture(null);
            }
            closeFuture = this.closeFuture;
        }
        return closeFuture;
    }
}
