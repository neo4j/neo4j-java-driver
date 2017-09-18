/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.net.pooling.PoolSettings;
import org.neo4j.driver.internal.net.pooling.SocketConnectionPool;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.spi.PooledConnection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AuthToken;
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
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.v1.Config.defaultConfig;
import static org.neo4j.driver.v1.Values.parameters;

public class ConnectionHandlingIT
{
    @ClassRule
    public static final TestNeo4j neo4j = new TestNeo4j();

    private Driver driver;
    private MemorizingConnectionPool connectionPool;

    @Before
    public void createDriver()
    {
        DriverFactoryWithConnector driverFactory = new DriverFactoryWithConnector();
        AuthToken auth = neo4j.authToken();
        RoutingSettings routingSettings = new RoutingSettings( 1, 1, null );
        RetrySettings retrySettings = RetrySettings.DEFAULT;
        driver = driverFactory.newInstance( neo4j.uri(), auth, routingSettings, retrySettings, defaultConfig() );
        connectionPool = driverFactory.connectionPool;
        connectionPool.startMemorizing(); // start memorizing connections after driver creation
    }

    @After
    public void closeDriver()
    {
        driver.close();
    }

    @Test
    public void connectionUsedForSessionRunReturnedToThePoolWhenResultConsumed()
    {
        StatementResult result = createNodesInNewSession( 12 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        result.consume();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).close();
    }

    @Test
    public void connectionUsedForSessionRunReturnedToThePoolWhenResultSummaryObtained()
    {
        StatementResult result = createNodesInNewSession( 5 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        ResultSummary summary = result.summary();

        assertEquals( 5, summary.counters().nodesCreated() );
        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).close();
    }

    @Test
    public void connectionUsedForSessionRunReturnedToThePoolWhenResultFetchedInList()
    {
        StatementResult result = createNodesInNewSession( 2 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        List<Record> records = result.list();
        assertEquals( 2, records.size() );

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).close();
    }

    @Test
    public void connectionUsedForSessionRunReturnedToThePoolWhenSingleRecordFetched()
    {
        StatementResult result = createNodesInNewSession( 1 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        assertNotNull( result.single() );

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).close();
    }

    @Test
    public void connectionUsedForSessionRunReturnedToThePoolWhenResultFetchedAsIterator()
    {
        StatementResult result = createNodesInNewSession( 6 );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        int seenRecords = 0;
        while ( result.hasNext() )
        {
            assertNotNull( result.next() );
            seenRecords++;
        }
        assertEquals( 6, seenRecords );

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).close();
    }

    @Test
    public void connectionUsedForSessionRunReturnedToThePoolWhenServerErrorDuringResultFetching()
    {
        Session session = driver.session();
        // provoke division by zero
        StatementResult result = session.run( "UNWIND range(10, 0, -1) AS i CREATE (n {index: 10/i}) RETURN n" );

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        try
        {
            result.hasNext();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).close();
    }

    @Test
    public void previousSessionRunResultIsBufferedBeforeRunningNewStatement()
    {
        Session session = driver.session();

        StatementResult result1 = createNodes( 3, session );
        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        StatementResult result2 = createNodes( 2, session );
        verify( connection1 ).close();

        assertEquals( 3, result1.list().size() );
        assertEquals( 2, result2.list().size() );
    }

    @Test
    public void previousSessionRunResultIsBufferedBeforeStartingNewTransaction()
    {
        Session session = driver.session();

        StatementResult result1 = createNodes( 3, session );
        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        session.beginTransaction();
        verify( connection1 ).close();

        assertEquals( 3, result1.list().size() );
    }

    @Test
    public void connectionUsedForTransactionReturnedToThePoolWhenTransactionCommitted()
    {
        Session session = driver.session();

        Transaction tx = session.beginTransaction();

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        StatementResult result = createNodes( 5, tx );
        tx.success();
        tx.close();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).close();

        assertEquals( 5, result.list().size() );
    }

    @Test
    public void connectionUsedForTransactionReturnedToThePoolWhenTransactionRolledBack()
    {
        Session session = driver.session();

        Transaction tx = session.beginTransaction();

        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1, never() ).close();

        StatementResult result = createNodes( 8, tx );
        tx.failure();
        tx.close();

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        verify( connection1 ).close();

        assertEquals( 8, result.list().size() );
    }

    @Test
    public void connectionUsedForTransactionReturnedToThePoolWhenTransactionFailsToCommitted() throws Exception
    {
        try ( Session session = driver.session() )
        {
            session.run( "CREATE CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)" );
        }

        Session session = driver.session();

        Transaction tx = session.beginTransaction();
        Connection connection1 = connectionPool.lastAcquiredConnectionSpy;
        verify( connection1 ).close(); // connection previously used for constraint creation

        // property existence constraints are verified on commit, try to violate it
        tx.run( "CREATE (:Book)" );
        tx.success();

        try
        {
            tx.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }

        Connection connection2 = connectionPool.lastAcquiredConnectionSpy;
        assertSame( connection1, connection2 );
        // connection should have been closed twice: for constraint creation and for node creation
        verify( connection1, times( 2 ) ).close();
    }

    @Test
    public void connectionDisposedWhenItHasUnrecoverableError()
    {
        Session session = driver.session();

        PooledConnection connection1;
        StatementResult result1;
        try ( Transaction tx = session.beginTransaction() )
        {
            result1 = tx.run( "RETURN 42 AS answer" );
            tx.success();
            connection1 = connectionPool.lastAcquiredConnectionSpy;
            when( connection1.hasUnrecoverableErrors() ).thenReturn( true );
        }

        verify( connection1 ).dispose();
        assertEquals( 42, result1.single().get( "answer" ).asInt() );

        PooledConnection connection2;
        StatementResult result2;
        try ( Transaction tx = session.beginTransaction() )
        {
            result2 = tx.run( "RETURN 4242 AS answer" );
            tx.success();
            connection2 = connectionPool.lastAcquiredConnectionSpy;
            assertNotSame( connection1, connection2 );
        }

        verify( connection2, never() ).dispose();
        verify( connection2 ).close();
        assertEquals( 4242, result2.single().get( "answer" ).asInt() );
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

    private static class DriverFactoryWithConnector extends DriverFactory
    {
        MemorizingConnectionPool connectionPool;

        @Override
        protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Config config )
        {
            ConnectionSettings connectionSettings = new ConnectionSettings( authToken, 1000 );
            PoolSettings poolSettings = new PoolSettings( config.maxIdleConnectionPoolSize(),
                    config.idleTimeBeforeConnectionTest(), config.maxConnectionLifetimeMillis(),
                    config.maxConnectionPoolSize(), config.connectionAcquisitionTimeoutMillis() );
            Connector connector = createConnector( connectionSettings, securityPlan, config.logging() );
            connectionPool = new MemorizingConnectionPool( poolSettings, connector, createClock(), config.logging() );
            return connectionPool;
        }
    }

    private static class MemorizingConnectionPool extends SocketConnectionPool
    {
        PooledConnection lastAcquiredConnectionSpy;
        boolean memorize;

        MemorizingConnectionPool( PoolSettings poolSettings, Connector connector, Clock clock, Logging logging )
        {
            super( poolSettings, connector, clock, logging );
        }

        void startMemorizing()
        {
            memorize = true;
        }

        @Override
        public PooledConnection acquire( BoltServerAddress address )
        {
            PooledConnection connection = super.acquire( address );

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

            return connection;
        }
    }
}
