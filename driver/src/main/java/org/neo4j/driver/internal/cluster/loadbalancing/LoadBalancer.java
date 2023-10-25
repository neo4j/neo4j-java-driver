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
package org.neo4j.driver.internal.cluster.loadbalancing;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.ConnectionContext.PENDING_DATABASE_NAME_EXCEPTION_SUPPLIER;
import static org.neo4j.driver.internal.async.ImmutableConnectionContext.simple;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.completionExceptionCause;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.Futures.onErrorContinue;

import io.netty.util.concurrent.EventExecutorGroup;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.async.connection.RoutingConnection;
import org.neo4j.driver.internal.cluster.Rediscovery;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.cluster.RoutingTable;
import org.neo4j.driver.internal.cluster.RoutingTableRegistry;
import org.neo4j.driver.internal.cluster.RoutingTableRegistryImpl;
import org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.SessionAuthUtil;

public class LoadBalancer implements ConnectionProvider {
    private static final String CONNECTION_ACQUISITION_COMPLETION_FAILURE_MESSAGE =
            "Connection acquisition failed for all available addresses.";
    private static final String CONNECTION_ACQUISITION_COMPLETION_EXCEPTION_MESSAGE =
            "Failed to obtain connection towards %s server. Known routing table is: %s";
    private static final String CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE =
            "Failed to obtain a connection towards address %s, will try other addresses if available. Complete failure is reported separately from this entry.";
    private final ConnectionPool connectionPool;
    private final RoutingTableRegistry routingTables;
    private final LoadBalancingStrategy loadBalancingStrategy;
    private final EventExecutorGroup eventExecutorGroup;
    private final Logger log;
    private final Rediscovery rediscovery;

    public LoadBalancer(
            ConnectionPool connectionPool,
            Rediscovery rediscovery,
            RoutingSettings settings,
            LoadBalancingStrategy loadBalancingStrategy,
            EventExecutorGroup eventExecutorGroup,
            Clock clock,
            Logging logging) {
        this(
                connectionPool,
                createRoutingTables(connectionPool, rediscovery, settings, clock, logging),
                rediscovery,
                loadBalancingStrategy,
                eventExecutorGroup,
                logging);
    }

    LoadBalancer(
            ConnectionPool connectionPool,
            RoutingTableRegistry routingTables,
            Rediscovery rediscovery,
            LoadBalancingStrategy loadBalancingStrategy,
            EventExecutorGroup eventExecutorGroup,
            Logging logging) {
        requireNonNull(rediscovery, "rediscovery must not be null");
        this.connectionPool = connectionPool;
        this.routingTables = routingTables;
        this.rediscovery = rediscovery;
        this.loadBalancingStrategy = loadBalancingStrategy;
        this.eventExecutorGroup = eventExecutorGroup;
        this.log = logging.getLog(getClass());
    }

    @Override
    public CompletionStage<Connection> acquireConnection(ConnectionContext context) {
        return routingTables.ensureRoutingTable(context).thenCompose(handler -> acquire(
                        context.mode(), handler.routingTable(), context.overrideAuthToken())
                .thenApply(connection -> new RoutingConnection(
                        connection,
                        Futures.joinNowOrElseThrow(
                                context.databaseNameFuture(), PENDING_DATABASE_NAME_EXCEPTION_SUPPLIER),
                        context.mode(),
                        context.impersonatedUser(),
                        handler)));
    }

    @Override
    public CompletionStage<Void> verifyConnectivity() {
        return this.supportsMultiDb()
                .thenCompose(supports -> routingTables.ensureRoutingTable(simple(supports)))
                .handle((ignored, error) -> {
                    if (error != null) {
                        var cause = completionExceptionCause(error);
                        if (cause instanceof ServiceUnavailableException) {
                            throw Futures.asCompletionException(new ServiceUnavailableException(
                                    "Unable to connect to database management service, ensure the database is running and that there is a working network connection to it.",
                                    cause));
                        }
                        throw Futures.asCompletionException(cause);
                    }
                    return null;
                });
    }

    @Override
    public CompletionStage<Void> close() {
        return connectionPool.close();
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDb() {
        return detectFeature(
                "Failed to perform multi-databases feature detection with the following servers: ",
                MultiDatabaseUtil::supportsMultiDatabase);
    }

    @Override
    public CompletionStage<Boolean> supportsSessionAuth() {
        return detectFeature(
                "Failed to perform session auth feature detection with the following servers: ",
                SessionAuthUtil::supportsSessionAuth);
    }

    private CompletionStage<Boolean> detectFeature(
            String baseErrorMessagePrefix, Function<Connection, Boolean> featureDetectionFunction) {
        List<BoltServerAddress> addresses;

        try {
            addresses = rediscovery.resolve();
        } catch (Throwable error) {
            return failedFuture(error);
        }
        CompletableFuture<Boolean> result = completedWithNull();
        Throwable baseError = new ServiceUnavailableException(baseErrorMessagePrefix + addresses);

        for (var address : addresses) {
            result = onErrorContinue(result, baseError, completionError -> {
                // We fail fast on security errors
                var error = completionExceptionCause(completionError);
                if (error instanceof SecurityException) {
                    return failedFuture(error);
                }
                return connectionPool.acquire(address, null).thenCompose(conn -> {
                    boolean featureDetected = featureDetectionFunction.apply(conn);
                    return conn.release().thenApply(ignored -> featureDetected);
                });
            });
        }
        return onErrorContinue(result, baseError, completionError -> {
            // If we failed with security errors, then we rethrow the security error out, otherwise we throw the chained
            // errors.
            var error = completionExceptionCause(completionError);
            if (error instanceof SecurityException) {
                return failedFuture(error);
            }
            return failedFuture(baseError);
        });
    }

    public RoutingTableRegistry getRoutingTableRegistry() {
        return routingTables;
    }

    private CompletionStage<Connection> acquire(
            AccessMode mode, RoutingTable routingTable, AuthToken overrideAuthToken) {
        var result = new CompletableFuture<Connection>();
        List<Throwable> attemptExceptions = new ArrayList<>();
        acquire(mode, routingTable, result, overrideAuthToken, attemptExceptions);
        return result;
    }

    private void acquire(
            AccessMode mode,
            RoutingTable routingTable,
            CompletableFuture<Connection> result,
            AuthToken overrideAuthToken,
            List<Throwable> attemptErrors) {
        var addresses = getAddressesByMode(mode, routingTable);
        var address = selectAddress(mode, addresses);

        if (address == null) {
            var completionError = new SessionExpiredException(
                    format(CONNECTION_ACQUISITION_COMPLETION_EXCEPTION_MESSAGE, mode, routingTable));
            attemptErrors.forEach(completionError::addSuppressed);
            log.error(CONNECTION_ACQUISITION_COMPLETION_FAILURE_MESSAGE, completionError);
            result.completeExceptionally(completionError);
            return;
        }

        connectionPool.acquire(address, overrideAuthToken).whenComplete((connection, completionError) -> {
            var error = completionExceptionCause(completionError);
            if (error != null) {
                if (error instanceof ServiceUnavailableException) {
                    var attemptMessage = format(CONNECTION_ACQUISITION_ATTEMPT_FAILURE_MESSAGE, address);
                    log.warn(attemptMessage);
                    log.debug(attemptMessage, error);
                    attemptErrors.add(error);
                    routingTable.forget(address);
                    eventExecutorGroup
                            .next()
                            .execute(() -> acquire(mode, routingTable, result, overrideAuthToken, attemptErrors));
                } else {
                    result.completeExceptionally(error);
                }
            } else {
                result.complete(connection);
            }
        });
    }

    private static List<BoltServerAddress> getAddressesByMode(AccessMode mode, RoutingTable routingTable) {
        return switch (mode) {
            case READ -> routingTable.readers();
            case WRITE -> routingTable.writers();
        };
    }

    private BoltServerAddress selectAddress(AccessMode mode, List<BoltServerAddress> addresses) {
        return switch (mode) {
            case READ -> loadBalancingStrategy.selectReader(addresses);
            case WRITE -> loadBalancingStrategy.selectWriter(addresses);
        };
    }

    private static RoutingTableRegistry createRoutingTables(
            ConnectionPool connectionPool,
            Rediscovery rediscovery,
            RoutingSettings settings,
            Clock clock,
            Logging logging) {
        return new RoutingTableRegistryImpl(
                connectionPool, rediscovery, clock, logging, settings.routingTablePurgeDelayMs());
    }

    /**
     * <b>This method is only for testing</b>
     */
    public Rediscovery getRediscovery() {
        return rediscovery;
    }
}
