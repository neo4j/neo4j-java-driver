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

import io.netty.bootstrap.Bootstrap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.QueryRunner;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.async.pool.PoolSettings;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.Config.defaultConfig;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.metrics.InternalAbstractMetrics.DEV_NULL_METRICS;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V4;
import static org.neo4j.driver.util.TestUtil.await;

@ParallelizableIT
class ConnectionHandlingIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;
    private MemorizingConnectionPool connectionPool;

    @BeforeEach
    void createDriver()
    {
        DriverFactoryWithConnectionPool driverFactory = new DriverFactoryWithConnectionPool();
        AuthToken auth = neo4j.authToken();
        RoutingSettings routingSettings = RoutingSettings.DEFAULT;
        RetrySettings retrySettings = RetrySettings.DEFAULT;
        driver = driverFactory.newInstance( neo4j.uri(), auth, routingSettings, retrySettings, defaultConfig(), SecurityPlanImpl.insecure() );
        connectionPool = driverFactory.connectionPool;
        connectionPool.startMemorizing(); // start memorizing connections after driver creation
    }

    @AfterEach
    void closeDriver()
    {
        driver.close();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenResultConsumed()
    {
        Result result = createNodesInNewSession( 12 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        result.consume();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenResultSummaryObtained()
    {
        Result result = createNodesInNewSession( 5 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        ResultSummary summary = result.consume();

        assertEquals( 5, summary.counters().nodesCreated() );
        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenResultFetchedInList()
    {
        Result result = createNodesInNewSession( 2 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        List<Record> records = result.list();
        assertEquals( 2, records.size() );

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenSingleRecordFetched()
    {
        Result result = createNodesInNewSession( 1 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        assertNotNull( result.single() );

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenResultFetchedAsIterator()
    {
        Result result = createNodesInNewSession( 6 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        int seenRecords = 0;
        while ( result.hasNext() )
        {
            assertNotNull( result.next() );
            seenRecords++;
        }
        assertEquals( 6, seenRecords );

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenServerErrorDuringResultFetching()
    {
        Session session = driver.session();
        // provoke division by zero
        Result result = session.run( "UNWIND range(10, 0, -1) AS i CREATE (n {index: 10/i}) RETURN n" );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        assertThrows( ClientException.class, result::consume );

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();
    }

    @Test
    void connectionUsedForTransactionReturnedToThePoolWhenTransactionCommitted()
    {
        Session session = driver.session();

        Transaction tx = session.beginTransaction();

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        Result result = createNodes( 5, tx );
        int size = result.list().size();
        tx.commit();
        tx.close();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();

        assertEquals( 5, size );
    }

    @Test
    void connectionUsedForTransactionReturnedToThePoolWhenTransactionRolledBack()
    {
        Session session = driver.session();

        Transaction tx = session.beginTransaction();

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        Result result = createNodes( 8, tx );
        int size = result.list().size();
        tx.rollback();
        tx.close();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();

        assertEquals( 8, size );
    }

    @Test
    void connectionUsedForTransactionReturnedToThePoolWhenTransactionFailsToCommitted() throws Exception
    {
        try ( Session session = driver.session() )
        {
            session.run( "CREATE CONSTRAINT ON (book:Library) ASSERT exists(book.isbn)" );
        }

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, atLeastOnce() ).release(); // connection used for constraint creation

        Session session = driver.session();
        Transaction tx = session.beginTransaction();
        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection2, never() ).release();

        // property existence constraints are verified on commit, try to violate it
        tx.run( "CREATE (:Library)" );

        assertThrows( ClientException.class, tx::commit );

        // connection should have been released after failed node creation
        verify( connection2 ).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenSessionClose()
    {
        Session session = driver.session();
        Result result = createNodes( 12, session );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        session.close();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1, times( 2 ) ).release();
    }

    @Test
    void connectionUsedForBeginTxReturnedToThePoolWhenSessionClose()
    {
        Session session = driver.session();
        Transaction tx = session.beginTransaction();

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        session.close();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1, times( 2 ) ).release();
    }

    @Test
    @EnabledOnNeo4jWith( BOLT_V4 )
    void sessionCloseShouldReleaseConnectionUsedBySessionRun() throws Throwable
    {
        RxSession session = driver.rxSession();
        RxResult res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );

        // When we only run but not pull
        StepVerifier.create( Flux.from( res.keys() ) ).expectNext( singletonList( "a" ) ).verifyComplete();
        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        // Then we shall discard all results and commit
        StepVerifier.create( Mono.from( session.close() ) ).verifyComplete();
        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1, times( 2 ) ).release();
    }

    @Test
    @EnabledOnNeo4jWith( BOLT_V4 )
    void resultRecordsShouldReleaseConnectionUsedBySessionRun() throws Throwable
    {
        RxSession session = driver.rxSession();
        RxResult res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );
        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        assertNull( connection1 );

        // When we run and pull
        StepVerifier.create( Flux.from( res.records() ).map( record -> record.get( "a" ).asInt() ) )
                .expectNext( 1, 2, 3, 4 ).verifyComplete();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertNotSame( connection1, connection2 );
        verify( connection2 ).release();
    }

    @Test
    @EnabledOnNeo4jWith( BOLT_V4 )
    void resultSummaryShouldReleaseConnectionUsedBySessionRun() throws Throwable
    {
        RxSession session = driver.rxSession();
        RxResult res = session.run( "UNWIND [1,2,3,4] AS a RETURN a" );
        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        assertNull( connection1 );

        StepVerifier.create( Mono.from( res.consume() ) ).expectNextCount( 1 ).verifyComplete();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertNotSame( connection1, connection2 );
        verify( connection2 ).release();
    }

    @Test
    @EnabledOnNeo4jWith( BOLT_V4 )
    void txCommitShouldReleaseConnectionUsedByBeginTx() throws Throwable
    {
        RxSession session = driver.rxSession();

        StepVerifier.create( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
            verify( connection1, never() ).release();

            RxResult result = tx.run( "UNWIND [1,2,3,4] AS a RETURN a" );
            StepVerifier.create( Flux.from( result.records() ).map( record -> record.get( "a" ).asInt() ) )
                    .expectNext( 1, 2, 3, 4 ).verifyComplete();

            StepVerifier.create( Mono.from( tx.commit() ) ).verifyComplete();
            Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
            assertSame( connection1, connection2 );
            verify( connection1 ).release();

        } ) ).expectNextCount( 1 ).verifyComplete();
    }

    @Test
    @EnabledOnNeo4jWith( BOLT_V4 )
    void txRollbackShouldReleaseConnectionUsedByBeginTx() throws Throwable
    {
        RxSession session = driver.rxSession();

        StepVerifier.create( Mono.from( session.beginTransaction() ).doOnSuccess( tx -> {
            Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
            verify( connection1, never() ).release();

            RxResult result = tx.run( "UNWIND [1,2,3,4] AS a RETURN a" );
            StepVerifier.create( Flux.from( result.records() ).map( record -> record.get( "a" ).asInt() ) )
                    .expectNext( 1, 2, 3, 4 ).verifyComplete();

            StepVerifier.create( Mono.from( tx.rollback() ) ).verifyComplete();
            Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
            assertSame( connection1, connection2 );
            verify( connection1 ).release();

        } ) ).expectNextCount( 1 ).verifyComplete();
    }

    @Test
    @EnabledOnNeo4jWith( BOLT_V4 )
    void sessionCloseShouldReleaseConnectionUsedByBeginTx() throws Throwable
    {
        // Given
        RxSession session = driver.rxSession();
        Publisher<RxTransaction> tx = session.beginTransaction();

        // When we created a tx
        StepVerifier.create( Mono.from( tx ) ).expectNextCount( 1 ).verifyComplete();
        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        // Then we shall discard all results and commit
        StepVerifier.create( Mono.from( session.close() ) ).verifyComplete();
        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1, times( 2 ) ).release();
    }

    private Result createNodesInNewSession(int nodesToCreate )
    {
        return createNodes( nodesToCreate, driver.session() );
    }

    private Result createNodes(int nodesToCreate, QueryRunner queryRunner)
    {
        return queryRunner.run( "UNWIND range(1, $nodesToCreate) AS i CREATE (n {index: i}) RETURN n",
                parameters( "nodesToCreate", nodesToCreate ) );
    }

    private static class DriverFactoryWithConnectionPool extends DriverFactory
    {
        MemorizingConnectionPool connectionPool;

        @Override
        protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Bootstrap bootstrap,
                                                       MetricsProvider ignored, Config config, boolean ownsEventLoopGroup,
                                                       RoutingContext routingContext )
        {
            ConnectionSettings connectionSettings = new ConnectionSettings( authToken, "test", 1000 );
            PoolSettings poolSettings = new PoolSettings( config.maxConnectionPoolSize(),
                    config.connectionAcquisitionTimeoutMillis(), config.maxConnectionLifetimeMillis(),
                    config.idleTimeBeforeConnectionTest() );
            Clock clock = createClock();
            ChannelConnector connector = super.createConnector( connectionSettings, securityPlan, config, clock, routingContext );
            connectionPool = new MemorizingConnectionPool( connector, bootstrap, poolSettings, config.logging(), clock, ownsEventLoopGroup );
            return connectionPool;
        }
    }

    private static class MemorizingConnectionPool extends ConnectionPoolImpl
    {
        Connection lastAcquiredConnectionSpy;
        boolean memorize;

        MemorizingConnectionPool( ChannelConnector connector, Bootstrap bootstrap, PoolSettings settings,
                Logging logging, Clock clock, boolean ownsEventLoopGroup )
        {
            super( connector, bootstrap, settings, DEV_NULL_METRICS, logging, clock, ownsEventLoopGroup );
        }

        void startMemorizing()
        {
            memorize = true;
        }

        @Override
        public CompletionStage<Connection> acquire( final BoltServerAddress address )
        {
            Connection connection = await( super.acquire( address ) );

            if ( memorize )
            {
                // this connection pool returns spies so spies will be returned to the pool
                // prevent spying on spies...
                if ( !Mockito.mockingDetails( connection ).isSpy() )
                {
                    connection = spy( connection );
                }
                lastAcquiredConnectionSpy = connection;
            }

            return CompletableFuture.completedFuture( connection );
        }
    }
}
