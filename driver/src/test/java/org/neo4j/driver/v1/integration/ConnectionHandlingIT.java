/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1.integration;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.async.ChannelConnector;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;
import org.neo4j.driver.internal.async.pool.PoolSettings;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.ChannelTrackingDriverFactory;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementRunner;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.DatabaseExtension;
import org.neo4j.driver.v1.util.StubServer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.metrics.InternalAbstractMetrics.DEV_NULL_METRICS;
import static org.neo4j.driver.v1.Config.defaultConfig;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.StubServer.INSECURE_CONFIG;
import static org.neo4j.driver.v1.util.TestUtil.await;

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
        RoutingSettings routingSettings = new RoutingSettings( 1, 1, null );
        RetrySettings retrySettings = RetrySettings.DEFAULT;
        driver = driverFactory.newInstance( neo4j.uri(), auth, routingSettings, retrySettings, defaultConfig() );
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
        StatementResult result = createNodesInNewSession( 12 );

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
        StatementResult result = createNodesInNewSession( 5 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        ResultSummary summary = result.summary();

        assertEquals( 5, summary.counters().nodesCreated() );
        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();
    }

    @Test
    void connectionUsedForSessionRunReturnedToThePoolWhenResultFetchedInList()
    {
        StatementResult result = createNodesInNewSession( 2 );

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
        StatementResult result = createNodesInNewSession( 1 );

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
        StatementResult result = createNodesInNewSession( 6 );

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
        StatementResult result = session.run( "UNWIND range(10, 0, -1) AS i CREATE (n {index: 10/i}) RETURN n" );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        assertThrows( ClientException.class, result::hasNext );

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

        StatementResult result = createNodes( 5, tx );
        tx.success();
        tx.close();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();

        assertEquals( 5, result.list().size() );
    }

    @Test
    void connectionUsedForTransactionReturnedToThePoolWhenTransactionRolledBack()
    {
        Session session = driver.session();

        Transaction tx = session.beginTransaction();

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).release();

        StatementResult result = createNodes( 8, tx );
        tx.failure();
        tx.close();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).release();

        assertEquals( 8, result.list().size() );
    }

    @Test
    void connectionUsedForTransactionReturnedToThePoolWhenTransactionFailsToCommitted() throws Exception
    {
        try ( Session session = driver.session() )
        {
            session.run( "CREATE CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)" );
        }

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, atLeastOnce() ).release(); // connection used for constraint creation

        Session session = driver.session();
        Transaction tx = session.beginTransaction();
        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection2, never() ).release();

        // property existence constraints are verified on commit, try to violate it
        tx.run( "CREATE (:Book)" );
        tx.success();

        assertThrows( ClientException.class, tx::close );

        // connection should have been released after failed node creation
        verify( connection2 ).release();
    }

    @Test
    void shouldCloseChannelWhenResetFails() throws Exception
    {
        StubServer server = StubServer.start( "reset_error.script", 9001 );
        try
        {
            URI uri = URI.create( "bolt://localhost:9001" );
            ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );

            try ( Driver driver = driverFactory.newInstance( uri, AuthTokens.none(), RoutingSettings.DEFAULT, RetrySettings.DEFAULT, INSECURE_CONFIG ) )
            {
                try ( Session session = driver.session() )
                {
                    assertEquals( 42, session.run( "RETURN 42 AS answer" ).single().get( 0 ).asInt() );
                }

                List<Channel> channels = driverFactory.pollChannels();
                // there should be a single channel
                assertEquals( 1, channels.size() );
                // and it should be closed because it failed to RESET
                assertNull( channels.get( 0 ).closeFuture().get( 30, SECONDS ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    private StatementResult createNodesInNewSession( int nodesToCreate )
    {
        return createNodes( nodesToCreate, driver.session() );
    }

    private StatementResult createNodes( int nodesToCreate, StatementRunner statementRunner )
    {
        return statementRunner.run( "UNWIND range(1, {nodesToCreate}) AS i CREATE (n {index: i}) RETURN n",
                parameters( "nodesToCreate", nodesToCreate ) );
    }

    private static class DriverFactoryWithConnectionPool extends DriverFactory
    {
        MemorizingConnectionPool connectionPool;

        @Override
        protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Bootstrap bootstrap,
                MetricsListener metrics, Config config )
        {
            ConnectionSettings connectionSettings = new ConnectionSettings( authToken, 1000 );
            PoolSettings poolSettings = new PoolSettings( config.maxConnectionPoolSize(),
                    config.connectionAcquisitionTimeoutMillis(), config.maxConnectionLifetimeMillis(),
                    config.idleTimeBeforeConnectionTest() );
            Clock clock = createClock();
            ChannelConnector connector = super.createConnector( connectionSettings, securityPlan, config, clock );
            connectionPool =
                    new MemorizingConnectionPool( connector, bootstrap, poolSettings, config.logging(), clock );
            return connectionPool;
        }
    }

    private static class MemorizingConnectionPool extends ConnectionPoolImpl
    {
        Connection lastAcquiredConnectionSpy;
        boolean memorize;

        MemorizingConnectionPool( ChannelConnector connector, Bootstrap bootstrap, PoolSettings settings,
                Logging logging, Clock clock )
        {
            super( connector, bootstrap, settings, DEV_NULL_METRICS, logging, clock );
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
