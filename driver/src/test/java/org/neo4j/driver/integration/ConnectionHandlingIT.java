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
package org.neo4j.driver.integration;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;
import static org.neo4j.driver.testutil.TestUtil.await;

import io.netty.bootstrap.Bootstrap;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logging;
import org.neo4j.driver.QueryRunner;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.BoltAgentUtil;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.async.pool.PoolSettings;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.metrics.DevNullMetricsListener;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ParallelizableIT
class ConnectionHandlingIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;
    private MemorizingConnectionPool connectionPool;

    @BeforeEach
    void createDriver() {
        var driverFactory = new DriverFactoryWithConnectionPool();
        var authTokenProvider = neo4j.authTokenManager();
        driver = driverFactory.newInstance(
                neo4j.uri(),
                authTokenProvider,
                Config.builder().withFetchSize(1).build(),
                SecurityPlanImpl.insecure(),
                null,
                null);
        connectionPool = driverFactory.connectionPool;
        connectionPool.startMemorizing(); // start memorizing connections after driver creation
    }

    @AfterEach
    void closeDriver() {
        driver.close();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenResultConsumed() {
        var result = createNodesInNewSession(12);

        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        result.consume();

        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenResultSummaryObtained() {
        var result = createNodesInNewSession(5);

        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        var summary = result.consume();

        assertEquals(5, summary.counters().nodesCreated());
        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenResultFetchedInList() {
        var result = createNodesInNewSession(2);

        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        var records = result.list();
        assertEquals(2, records.size());

        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenSingleRecordFetched() {
        var result = createNodesInNewSession(1);

        assertNotNull(result.single());

        var connection = connectionPool.lastAcquiredConnectionSpy;
        verify(connection).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenResultFetchedAsIterator() {
        var result = createNodesInNewSession(6);

        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        var seenRecords = 0;
        while (result.hasNext()) {
            assertNotNull(result.next());
            seenRecords++;
        }
        assertEquals(6, seenRecords);

        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolOnServerFailure() {
        try (var session = driver.session()) {
            // provoke division by zero
            assertThrows(ClientException.class, () -> session.run(
                            "UNWIND range(10, -1, 0) AS i CREATE (n {index: 10/i}) RETURN n")
                    .consume());

            var connection1 = connectionPool.lastAcquiredConnectionSpy;
            verify(connection1).release();
        }
    }

    @Test
    void connectionUsedForTransactionReturnedToThePoolWhenTransactionCommitted() {
        var session = driver.session();

        var tx = session.beginTransaction();

        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        var result = createNodes(5, tx);
        var size = result.list().size();
        tx.commit();
        tx.close();

        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1).release();

        assertEquals(5, size);
    }

    @Test
    void connectionUsedForTransactionReturnedToThePoolWhenTransactionRolledBack() {
        var session = driver.session();

        var tx = session.beginTransaction();

        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        var result = createNodes(8, tx);
        var size = result.list().size();
        tx.rollback();
        tx.close();

        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1).release();

        assertEquals(8, size);
    }

    @Test
    void connectionUsedForTransactionReturnedToThePoolWhenTransactionFailsToCommitted() {
        try (var session = driver.session()) {
            if (neo4j.isNeo4j43OrEarlier()) {
                session.run("CREATE CONSTRAINT ON (book:Library) ASSERT exists(book.isbn)");
            } else {
                session.run("CREATE CONSTRAINT FOR (book:Library) REQUIRE book.isbn IS NOT NULL");
            }
        }

        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, atLeastOnce()).release(); // connection used for constraint creation

        var session = driver.session();
        var tx = session.beginTransaction();
        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection2, never()).release();

        // property existence constraints are verified on commit, try to violate it
        tx.run("CREATE (:Library)");

        assertThrows(ClientException.class, tx::commit);

        // connection should have been released after failed node creation
        verify(connection2).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenSessionClose() {
        var session = driver.session();
        createNodes(12, session);

        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        session.close();

        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1, times(2)).release();
    }

    @Test
    void connectionUsedForBeginTxReturnedToThePoolWhenSessionClose() {
        var session = driver.session();
        session.beginTransaction();

        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        session.close();

        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1, times(2)).release();
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("deprecation")
    void sessionCloseShouldReleaseConnectionUsedBySessionRun() {
        var session = driver.rxSession();
        var res = session.run("UNWIND [1,2,3,4] AS a RETURN a");

        // When we only run but not pull
        StepVerifier.create(Flux.from(res.keys()))
                .expectNext(singletonList("a"))
                .verifyComplete();
        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        // Then we shall discard all results and commit
        StepVerifier.create(Mono.from(session.close())).verifyComplete();
        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1, times(2)).release();
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("deprecation")
    void resultRecordsShouldReleaseConnectionUsedBySessionRun() {
        var session = driver.rxSession();
        var res = session.run("UNWIND [1,2,3,4] AS a RETURN a");
        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        assertNull(connection1);

        // When we run and pull
        StepVerifier.create(
                        Flux.from(res.records()).map(record -> record.get("a").asInt()))
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertNotNull(connection2);
        verify(connection2).release();
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("deprecation")
    void resultSummaryShouldReleaseConnectionUsedBySessionRun() {
        var session = driver.rxSession();
        var res = session.run("UNWIND [1,2,3,4] AS a RETURN a");
        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        assertNull(connection1);

        StepVerifier.create(Mono.from(res.consume())).expectNextCount(1).verifyComplete();

        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertNotNull(connection2);
        verify(connection2).release();
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("deprecation")
    void txCommitShouldReleaseConnectionUsedByBeginTx() {
        var connection1Ref = new AtomicReference<Connection>();

        Function<RxSession, Publisher<Record>> sessionToRecordPublisher = (RxSession session) -> Flux.usingWhen(
                Mono.fromDirect(session.beginTransaction()),
                tx -> {
                    connection1Ref.set(connectionPool.lastAcquiredConnectionSpy);
                    verify(connection1Ref.get(), never()).release();
                    return tx.run("UNWIND [1,2,3,4] AS a RETURN a").records();
                },
                RxTransaction::commit,
                (tx, error) -> tx.rollback(),
                RxTransaction::rollback);

        var resultsFlux = Flux.usingWhen(
                        Mono.fromSupplier(driver::rxSession),
                        sessionToRecordPublisher,
                        session -> {
                            var connection2 = connectionPool.lastAcquiredConnectionSpy;
                            assertSame(connection1Ref.get(), connection2);
                            verify(connection1Ref.get()).release();
                            return Mono.empty();
                        },
                        (session, error) -> session.close(),
                        RxSession::close)
                .map(record -> record.get("a").asInt());

        StepVerifier.create(resultsFlux).expectNext(1, 2, 3, 4).expectComplete().verify();
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("deprecation")
    void txRollbackShouldReleaseConnectionUsedByBeginTx() {
        var connection1Ref = new AtomicReference<Connection>();

        Function<RxSession, Publisher<Record>> sessionToRecordPublisher = (RxSession session) -> Flux.usingWhen(
                Mono.fromDirect(session.beginTransaction()),
                tx -> {
                    connection1Ref.set(connectionPool.lastAcquiredConnectionSpy);
                    verify(connection1Ref.get(), never()).release();
                    return tx.run("UNWIND [1,2,3,4] AS a RETURN a").records();
                },
                RxTransaction::rollback,
                (tx, error) -> tx.rollback(),
                RxTransaction::rollback);

        var resultsFlux = Flux.usingWhen(
                        Mono.fromSupplier(driver::rxSession),
                        sessionToRecordPublisher,
                        session -> {
                            var connection2 = connectionPool.lastAcquiredConnectionSpy;
                            assertSame(connection1Ref.get(), connection2);
                            verify(connection1Ref.get()).release();
                            return Mono.empty();
                        },
                        (session, error) -> session.close(),
                        RxSession::close)
                .map(record -> record.get("a").asInt());

        StepVerifier.create(resultsFlux).expectNext(1, 2, 3, 4).expectComplete().verify();
    }

    @Test
    @EnabledOnNeo4jWith(BOLT_V4)
    @SuppressWarnings("deprecation")
    void sessionCloseShouldReleaseConnectionUsedByBeginTx() {
        // Given
        var session = driver.rxSession();
        var tx = session.beginTransaction();

        // When we created a tx
        StepVerifier.create(Mono.from(tx)).expectNextCount(1).verifyComplete();
        var connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify(connection1, never()).release();

        // Then we shall discard all results and commit
        StepVerifier.create(Mono.from(session.close())).verifyComplete();
        var connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame(connection1, connection2);
        verify(connection1, times(2)).release();
    }

    private Result createNodesInNewSession(int nodesToCreate) {
        return createNodes(nodesToCreate, driver.session());
    }

    private Result createNodes(int nodesToCreate, QueryRunner queryRunner) {
        return queryRunner.run(
                "UNWIND range(1, $nodesToCreate) AS i CREATE (n {index: i}) RETURN n",
                parameters("nodesToCreate", nodesToCreate));
    }

    private static class DriverFactoryWithConnectionPool extends DriverFactory {
        MemorizingConnectionPool connectionPool;

        @Override
        protected ConnectionPool createConnectionPool(
                AuthTokenManager authTokenManager,
                SecurityPlan securityPlan,
                Bootstrap bootstrap,
                MetricsProvider ignored,
                Config config,
                boolean ownsEventLoopGroup,
                RoutingContext routingContext) {
            var connectionSettings = new ConnectionSettings(authTokenManager, "test", 1000);
            var poolSettings = new PoolSettings(
                    config.maxConnectionPoolSize(),
                    config.connectionAcquisitionTimeoutMillis(),
                    config.maxConnectionLifetimeMillis(),
                    config.idleTimeBeforeConnectionTest());
            var clock = createClock();
            var connector = super.createNetworkConnector(
                    connectionSettings, securityPlan, config, clock, routingContext, BoltAgentUtil.VALUE);
            connectionPool = new MemorizingConnectionPool(
                    connector, bootstrap, poolSettings, config.logging(), clock, ownsEventLoopGroup);
            return connectionPool;
        }
    }

    private static class MemorizingConnectionPool extends ConnectionPoolImpl<InetSocketAddress> {
        Connection lastAcquiredConnectionSpy;
        boolean memorize;

        MemorizingConnectionPool(
                ChannelConnector<InetSocketAddress> connector,
                Bootstrap bootstrap,
                PoolSettings settings,
                Logging logging,
                Clock clock,
                boolean ownsEventLoopGroup) {
            super(connector, bootstrap, settings, DevNullMetricsListener.INSTANCE, logging, clock, ownsEventLoopGroup);
        }

        void startMemorizing() {
            memorize = true;
        }

        @Override
        public CompletionStage<Connection> acquire(final InetSocketAddress address, AuthToken overrideAuthToken) {
            var connection = await(super.acquire(address, overrideAuthToken));

            if (memorize) {
                // this connection pool returns spies so spies will be returned to the pool
                // prevent spying on spies...
                if (!Mockito.mockingDetails(connection).isSpy()) {
                    connection = spy(connection);
                }
                lastAcquiredConnectionSpy = connection;
            }

            return CompletableFuture.completedFuture(connection);
        }
    }
}
