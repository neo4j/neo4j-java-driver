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
package org.neo4j.driver.internal.bolt.routedimpl;

import static java.lang.String.format;
import static org.neo4j.driver.internal.bolt.routedimpl.util.LockUtil.executeWithLock;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.bolt.api.BoltConnectionProvider;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.MetricsListener;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.Rediscovery;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.RediscoveryImpl;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.RoutingTable;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.RoutingTableHandler;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.RoutingTableRegistry;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.RoutingTableRegistryImpl;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.loadbalancing.LoadBalancingStrategy;
import org.neo4j.driver.internal.bolt.routedimpl.util.FutureUtil;

public class RoutedBoltConnectionProvider implements BoltConnectionProvider {
    private static final String CONNECTION_ACQUISITION_COMPLETION_FAILURE_MESSAGE =
            "Connection acquisition failed for all available addresses.";
    private static final String CONNECTION_ACQUISITION_COMPLETION_EXCEPTION_MESSAGE =
            "Failed to obtain connection towards %s server. Known routing table is: %s";
    private static final String CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE =
            "Failed to obtain a connection towards address %s, will try other addresses if available. Complete failure is reported separately from this entry.";
    private final LoggingProvider logging;
    private final System.Logger log;
    private final ReentrantLock lock = new ReentrantLock();
    private final Supplier<BoltConnectionProvider> boltConnectionProviderSupplier;

    private final Map<BoltServerAddress, BoltConnectionProvider> addressToProvider = new HashMap<>();
    private final Function<BoltServerAddress, Set<BoltServerAddress>> resolver;
    private final DomainNameResolver domainNameResolver;
    private final Map<BoltServerAddress, Integer> addressToInUseCount = new HashMap<>();

    private final LoadBalancingStrategy loadBalancingStrategy;
    private final long routingTablePurgeDelayMs;

    private Rediscovery rediscovery;
    private RoutingTableRegistry registry;

    private BoltServerAddress address;

    private RoutingContext routingContext;
    private BoltAgent boltAgent;
    private String userAgent;
    private int connectTimeoutMillis;
    private CompletableFuture<Void> closeFuture;
    private final Clock clock;
    private MetricsListener metricsListener;

    public RoutedBoltConnectionProvider(
            Supplier<BoltConnectionProvider> boltConnectionProviderSupplier,
            Function<BoltServerAddress, Set<BoltServerAddress>> resolver,
            DomainNameResolver domainNameResolver,
            long routingTablePurgeDelayMs,
            Rediscovery rediscovery,
            Clock clock,
            LoggingProvider logging) {
        this.boltConnectionProviderSupplier = Objects.requireNonNull(boltConnectionProviderSupplier);
        this.resolver = Objects.requireNonNull(resolver);
        this.logging = Objects.requireNonNull(logging);
        this.log = logging.getLog(getClass());
        this.loadBalancingStrategy = new LeastConnectedLoadBalancingStrategy(
                (addr) -> {
                    synchronized (this) {
                        return addressToInUseCount.getOrDefault(address, 0);
                    }
                },
                logging);
        this.domainNameResolver = Objects.requireNonNull(domainNameResolver);
        this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
        this.rediscovery = rediscovery;
        this.clock = Objects.requireNonNull(clock);
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
        if (this.rediscovery == null) {
            this.rediscovery = new RediscoveryImpl(address, resolver, logging, domainNameResolver);
        }
        this.registry = new RoutingTableRegistryImpl(
                this::get, rediscovery, clock, logging, routingTablePurgeDelayMs, this::shutdownUnusedProviders);
        this.metricsListener = Objects.requireNonNull(metricsListener);

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

        var handlerRef = new AtomicReference<RoutingTableHandler>();
        var databaseNameFuture = databaseName == null
                ? new CompletableFuture<DatabaseName>()
                : CompletableFuture.completedFuture(databaseName);
        databaseNameFuture.whenComplete((name, throwable) -> {
            if (name != null) {
                databaseNameConsumer.accept(name);
            }
        });
        return registry.ensureRoutingTable(
                        securityPlan,
                        databaseNameFuture,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        authMapStageSupplier,
                        minVersion)
                .thenApply(routingTableHandler -> {
                    handlerRef.set(routingTableHandler);
                    return routingTableHandler;
                })
                .thenCompose(routingTableHandler -> acquire(
                        securityPlan,
                        mode,
                        routingTableHandler.routingTable(),
                        authMapStageSupplier,
                        routingTableHandler.routingTable().database(),
                        Set.of(),
                        impersonatedUser,
                        minVersion,
                        notificationConfig))
                .thenApply(boltConnection -> new RoutedBoltConnection(boltConnection, handlerRef.get(), mode, this));
    }

    @Override
    public CompletionStage<Void> verifyConnectivity(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return supportsMultiDb(securityPlan, authMap)
                .thenCompose(supports -> registry.ensureRoutingTable(
                        securityPlan,
                        supports
                                ? CompletableFuture.completedFuture(DatabaseNameUtil.database("system"))
                                : CompletableFuture.completedFuture(DatabaseNameUtil.defaultDatabase()),
                        AccessMode.READ,
                        Collections.emptySet(),
                        null,
                        () -> CompletableFuture.completedStage(authMap),
                        null))
                .handle((ignored, error) -> {
                    if (error != null) {
                        var cause = FutureUtil.completionExceptionCause(error);
                        if (cause instanceof ServiceUnavailableException) {
                            throw FutureUtil.asCompletionException(new ServiceUnavailableException(
                                    "Unable to connect to database management service, ensure the database is running and that there is a working network connection to it.",
                                    cause));
                        }
                        throw FutureUtil.asCompletionException(cause);
                    }
                    return null;
                });
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return detectFeature(
                securityPlan,
                authMap,
                "Failed to perform multi-databases feature detection with the following servers: ",
                (boltConnection -> boltConnection.protocolVersion().compareTo(new BoltProtocolVersion(4, 0)) >= 0));
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth(SecurityPlan securityPlan, Map<String, Value> authMap) {
        return detectFeature(
                securityPlan,
                authMap,
                "Failed to perform session auth feature detection with the following servers: ",
                (boltConnection -> new BoltProtocolVersion(5, 1).compareTo(boltConnection.protocolVersion()) <= 0));
    }

    private synchronized void shutdownUnusedProviders(Set<BoltServerAddress> addressesToRetain) {
        var iterator = addressToProvider.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            var address = entry.getKey();
            if (!addressesToRetain.contains(address) && addressToInUseCount.getOrDefault(address, 0) == 0) {
                entry.getValue().close();
                iterator.remove();
            }
        }
    }

    private CompletionStage<Boolean> detectFeature(
            SecurityPlan securityPlan,
            Map<String, Value> authMap,
            String baseErrorMessagePrefix,
            Function<BoltConnection, Boolean> featureDetectionFunction) {
        List<BoltServerAddress> addresses;

        try {
            addresses = rediscovery.resolve();
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        CompletableFuture<Boolean> result = CompletableFuture.completedFuture(null);
        Throwable baseError = new ServiceUnavailableException(
                "Failed to perform multi-databases feature detection with the following servers: " + addresses);

        for (var address : addresses) {
            result = FutureUtil.onErrorContinue(result, baseError, completionError -> {
                // We fail fast on security errors
                var error = FutureUtil.completionExceptionCause(completionError);
                if (error instanceof SecurityException) {
                    return CompletableFuture.failedFuture(error);
                }
                return get(address)
                        .connect(
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
                            var featureDetected = featureDetectionFunction.apply(boltConnection);
                            return boltConnection.close().thenApply(ignored -> featureDetected);
                        });
            });
        }
        return FutureUtil.onErrorContinue(result, baseError, completionError -> {
            // If we failed with security errors, then we rethrow the security error out, otherwise we throw the chained
            // errors.
            var error = FutureUtil.completionExceptionCause(completionError);
            if (error instanceof SecurityException) {
                return CompletableFuture.failedFuture(error);
            }
            return CompletableFuture.failedFuture(baseError);
        });
    }

    private CompletionStage<BoltConnection> acquire(
            SecurityPlan securityPlan,
            AccessMode mode,
            RoutingTable routingTable,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            DatabaseName database,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig) {
        var result = new CompletableFuture<BoltConnection>();
        List<Throwable> attemptExceptions = new ArrayList<>();
        acquire(
                securityPlan,
                mode,
                routingTable,
                result,
                authMapStageSupplier,
                attemptExceptions,
                database,
                bookmarks,
                impersonatedUser,
                minVersion,
                notificationConfig);
        return result;
    }

    private void acquire(
            SecurityPlan securityPlan,
            AccessMode mode,
            RoutingTable routingTable,
            CompletableFuture<BoltConnection> result,
            Supplier<CompletionStage<Map<String, Value>>> authMapStageSupplier,
            List<Throwable> attemptErrors,
            DatabaseName database,
            Set<String> bookmarks,
            String impersonatedUser,
            BoltProtocolVersion minVersion,
            NotificationConfig notificationConfig) {
        var addresses = getAddressesByMode(mode, routingTable);
        log.log(System.Logger.Level.DEBUG, "Addresses: " + addresses);
        var address = selectAddress(mode, addresses);
        log.log(System.Logger.Level.DEBUG, "Selected address: " + address);

        if (address == null) {
            var completionError = new SessionExpiredException(
                    format(CONNECTION_ACQUISITION_COMPLETION_EXCEPTION_MESSAGE, mode, routingTable));
            attemptErrors.forEach(completionError::addSuppressed);
            log.log(System.Logger.Level.ERROR, CONNECTION_ACQUISITION_COMPLETION_FAILURE_MESSAGE, completionError);
            result.completeExceptionally(completionError);
            return;
        }

        get(address)
                .connect(
                        securityPlan,
                        database,
                        authMapStageSupplier,
                        mode,
                        bookmarks,
                        impersonatedUser,
                        minVersion,
                        notificationConfig,
                        (ignored) -> {})
                .whenComplete((connection, completionError) -> {
                    var error = FutureUtil.completionExceptionCause(completionError);
                    if (error != null) {
                        if (error instanceof ServiceUnavailableException) {
                            var attemptMessage = format(CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE, address);
                            log.log(System.Logger.Level.WARNING, attemptMessage);
                            log.log(System.Logger.Level.DEBUG, attemptMessage, error);
                            attemptErrors.add(error);
                            routingTable.forget(address);
                            CompletableFuture.runAsync(() -> acquire(
                                    securityPlan,
                                    mode,
                                    routingTable,
                                    result,
                                    authMapStageSupplier,
                                    attemptErrors,
                                    database,
                                    bookmarks,
                                    impersonatedUser,
                                    minVersion,
                                    notificationConfig));
                        } else {
                            result.completeExceptionally(error);
                        }
                    } else {
                        synchronized (this) {
                            var inUse = addressToInUseCount.getOrDefault(address, 0);
                            inUse++;
                            addressToInUseCount.put(address, inUse);
                        }
                        result.complete(connection);
                    }
                });
    }

    private BoltServerAddress selectAddress(AccessMode mode, List<BoltServerAddress> addresses) {
        return switch (mode) {
            case READ -> loadBalancingStrategy.selectReader(addresses);
            case WRITE -> loadBalancingStrategy.selectWriter(addresses);
        };
    }

    private static List<BoltServerAddress> getAddressesByMode(AccessMode mode, RoutingTable routingTable) {
        return switch (mode) {
            case READ -> routingTable.readers();
            case WRITE -> routingTable.writers();
        };
    }

    synchronized void decreaseCount(BoltServerAddress address) {
        var inUse = addressToInUseCount.get(address);
        if (inUse != null) {
            inUse--;
            if (inUse <= 0) {
                addressToInUseCount.remove(address);
            } else {
                addressToInUseCount.put(address, inUse);
            }
        }
    }

    @Override
    public CompletionStage<Void> close() {
        CompletableFuture<Void> closeFuture;
        synchronized (this) {
            if (this.closeFuture == null) {
                var futures = executeWithLock(lock, () -> addressToProvider.values().stream()
                        .map(BoltConnectionProvider::close)
                        .map(CompletionStage::toCompletableFuture)
                        .toArray(CompletableFuture[]::new));
                this.closeFuture = CompletableFuture.allOf(futures);
            }
            closeFuture = this.closeFuture;
        }
        return closeFuture;
    }

    private BoltConnectionProvider get(BoltServerAddress address) {
        return executeWithLock(lock, () -> {
            var provider = addressToProvider.get(address);
            if (provider == null) {
                provider = boltConnectionProviderSupplier.get();
                provider.init(address, routingContext, boltAgent, userAgent, connectTimeoutMillis, metricsListener);
                addressToProvider.put(address, provider);
            }
            return provider;
        });
    }
}
