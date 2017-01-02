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

import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.util.ConnectionTrackingDriverFactory;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.cc.Cluster;
import org.neo4j.driver.v1.util.cc.ClusterMember;
import org.neo4j.driver.v1.util.cc.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CausalClusteringIT
{
    private static final long DEFAULT_TIMEOUT_MS = 15_000;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    @Test
    public void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfLeader() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        int count = executeWriteAndReadThroughBolt( cluster.leader() );

        assertEquals( 1, count );
    }

    @Test
    public void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfFollower() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        int count = executeWriteAndReadThroughBolt( cluster.anyFollower() );

        assertEquals( 1, count );
    }

    @Test
    public void sessionCreationShouldFailIfCallingDiscoveryProcedureOnEdgeServer() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        ClusterMember readReplica = cluster.anyReadReplica();
        try
        {
            createDriver( readReplica.getRoutingUri() );
            fail( "Should have thrown an exception using a read replica address for routing" );
        }
        catch ( ServiceUnavailableException ex )
        {
            assertEquals( "Could not perform discovery. No routing servers available.", ex.getMessage() );
        }
    }

    // Ensure that Bookmarks work with single instances using a driver created using a bolt[not+routing] URI.
    @Test
    public void bookmarksShouldWorkWithDriverPinnedToSingleServer() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getBoltUri() ) )
        {
            String bookmark = inExpirableSession( driver, createSession(), new Function<Session,String>()
            {
                @Override
                public String apply( Session session )
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                        tx.success();
                    }

                    return session.lastBookmark();
                }
            } );

            assertNotNull( bookmark );

            try ( Session session = driver.session();
                  Transaction tx = session.beginTransaction( bookmark ) )
            {
                Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 1, record.get( "count" ).asInt() );
                tx.success();
            }
        }
    }

    @Test
    public void shouldUseBookmarkFromAReadSessionInAWriteSession() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getBoltUri() ) )
        {
            inExpirableSession( driver, createWritableSession(), new Function<Session,Void>()
            {
                @Override
                public Void apply( Session session )
                {
                    session.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                    return null;
                }
            } );

            final String bookmark;
            try ( Session session = driver.session( AccessMode.READ ) )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                    tx.success();
                }

                bookmark = session.lastBookmark();
            }

            assertNotNull( bookmark );

            inExpirableSession( driver, createWritableSession(), new Function<Session,Void>()
            {
                @Override
                public Void apply( Session session )
                {
                    try ( Transaction tx = session.beginTransaction( bookmark ) )
                    {
                        tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                        tx.success();
                    }

                    return null;
                }
            } );

            try ( Session session = driver.session() )
            {
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 2, record.get( "count" ).asInt() );
            }
        }
    }

    @Test
    public void shouldDropBrokenOldSessions() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        int concurrentSessionsCount = 9;
        int livenessCheckTimeoutMinutes = 2;

        Config config = Config.build()
                .withConnectionLivenessCheckTimeout( livenessCheckTimeoutMinutes, TimeUnit.MINUTES )
                .withEncryptionLevel( Config.EncryptionLevel.NONE )
                .toConfig();

        FakeClock clock = new FakeClock();
        ConnectionTrackingDriverFactory driverFactory = new ConnectionTrackingDriverFactory( clock );

        URI routingUri = cluster.leader().getRoutingUri();
        RoutingSettings routingSettings = new RoutingSettings( 1, TimeUnit.SECONDS.toMillis( 5 ) );
        AuthToken authToken = clusterRule.getDefaultAuthToken();

        try ( Driver driver = driverFactory.newInstance( routingUri, authToken, routingSettings, config ) )
        {
            // create nodes in different threads using different sessions
            createNodesInDifferentThreads( concurrentSessionsCount, driver );

            // now pool contains many sessions, make them all invalid
            driverFactory.closeConnections();
            // move clock forward more than configured liveness check timeout
            clock.progress( TimeUnit.MINUTES.toMillis( livenessCheckTimeoutMinutes + 1 ) );

            // now all idle connections should be considered too old and will be verified during acquisition
            // they will appear broken because they were closed and new valid connection will be created
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                List<Record> records = session.run( "MATCH (n) RETURN count(n)" ).list();
                assertEquals( 1, records.size() );
                assertEquals( concurrentSessionsCount, records.get( 0 ).get( 0 ).asInt() );
            }
        }
    }

    private int executeWriteAndReadThroughBolt( ClusterMember member ) throws TimeoutException, InterruptedException
    {
        try ( Driver driver = createDriver( member.getRoutingUri() ) )
        {
            return inExpirableSession( driver, createWritableSession(), executeWriteAndRead() );
        }
    }

    private Function<Driver,Session> createSession()
    {
        return new Function<Driver,Session>()
        {
            @Override
            public Session apply( Driver driver )
            {
                return driver.session();
            }
        };
    }

    private Function<Driver,Session> createWritableSession()
    {
        return new Function<Driver,Session>()
        {
            @Override
            public Session apply( Driver driver )
            {
                return driver.session( AccessMode.WRITE );
            }
        };
    }

    private Function<Session,Integer> executeWriteAndRead()
    {
        return new Function<Session,Integer>()
        {
            @Override
            public Integer apply( Session session )
            {
                session.run( "MERGE (n:Person {name: 'Jim'})" ).consume();
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                return record.get( "count" ).asInt();
            }
        };
    }

    private <T> T inExpirableSession( Driver driver, Function<Driver,Session> acquirer, Function<Session,T> op )
            throws TimeoutException, InterruptedException
    {
        long endTime = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        do
        {
            try ( Session session = acquirer.apply( driver ) )
            {
                return op.apply( session );
            }
            catch ( SessionExpiredException e )
            {
                // role might have changed; try again;
            }
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }

    private Driver createDriver( URI boltUri )
    {
        Logging devNullLogging = new Logging()
        {
            @Override
            public Logger getLog( String name )
            {
                return DevNullLogger.DEV_NULL_LOGGER;
            }
        };

        Config config = Config.build()
                .withLogging( devNullLogging )
                .toConfig();

        return GraphDatabase.driver( boltUri, clusterRule.getDefaultAuthToken(), config );
    }

    private static void createNodesInDifferentThreads( int count, final Driver driver ) throws Exception
    {
        final CountDownLatch beforeRunLatch = new CountDownLatch( count );
        final CountDownLatch runQueryLatch = new CountDownLatch( 1 );
        final ExecutorService executor = Executors.newCachedThreadPool();

        for ( int i = 0; i < count; i++ )
        {
            executor.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    beforeRunLatch.countDown();
                    try ( Session session = driver.session( AccessMode.WRITE ) )
                    {
                        runQueryLatch.await();
                        session.run( "CREATE ()" );
                    }
                    return null;
                }
            } );
        }

        beforeRunLatch.await();
        runQueryLatch.countDown();

        executor.shutdown();
        assertTrue( executor.awaitTermination( 1, TimeUnit.MINUTES ) );
    }
}
