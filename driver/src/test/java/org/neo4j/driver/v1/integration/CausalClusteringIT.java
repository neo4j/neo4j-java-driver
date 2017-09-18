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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.logging.DevNullLogger;
import org.neo4j.driver.internal.retry.RetrySettings;
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
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.cc.Cluster;
import org.neo4j.driver.v1.util.cc.ClusterMember;
import org.neo4j.driver.v1.util.cc.ClusterMemberRole;
import org.neo4j.driver.v1.util.cc.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.Values.parameters;

public class CausalClusteringIT
{
    private static final long DEFAULT_TIMEOUT_MS = 120_000;

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
    public void shouldExecuteReadAndWritesWhenRouterIsDiscovered() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        int count = executeWriteAndReadThroughBoltOnFirstAvailableAddress( cluster.anyReadReplica(), cluster.leader() );

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
            assertThat( ex.getMessage(), containsString( "Failed to run 'CALL dbms.cluster.routing" ) );
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

            try ( Session session = driver.session( bookmark );
                  Transaction tx = session.beginTransaction() )
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
            inExpirableSession( driver, createWritableSession( null ), new Function<Session,Void>()
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

            inExpirableSession( driver, createWritableSession( bookmark ), new Function<Session,Void>()
            {
                @Override
                public Void apply( Session session )
                {
                    try ( Transaction tx = session.beginTransaction() )
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
                .withoutEncryption()
                .toConfig();

        FakeClock clock = new FakeClock();
        ConnectionTrackingDriverFactory driverFactory = new ConnectionTrackingDriverFactory( clock );

        URI routingUri = cluster.leader().getRoutingUri();
        AuthToken auth = clusterRule.getDefaultAuthToken();
        RoutingSettings routingSettings = new RoutingSettings( 1, SECONDS.toMillis( 5 ), null );
        RetrySettings retrySettings = RetrySettings.DEFAULT;

        try ( Driver driver = driverFactory.newInstance( routingUri, auth, routingSettings, retrySettings, config ) )
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

    @Test
    public void beginTransactionThrowsForInvalidBookmark()
    {
        String invalidBookmark = "hi, this is an invalid bookmark";
        ClusterMember leader = clusterRule.getCluster().leader();

        try ( Driver driver = createDriver( leader.getBoltUri() );
              Session session = driver.session( invalidBookmark ) )
        {
            try
            {
                session.beginTransaction();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( ClientException.class ) );
                assertThat( e.getMessage(), containsString( invalidBookmark ) );
            }
        }
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void beginTransactionThrowsForUnreachableBookmark()
    {
        ClusterMember leader = clusterRule.getCluster().leader();

        try ( Driver driver = createDriver( leader.getBoltUri() );
              Session session = driver.session() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE ()" );
                tx.success();
            }

            String bookmark = session.lastBookmark();
            assertNotNull( bookmark );
            String newBookmark = bookmark + "0";

            try
            {
                session.beginTransaction( newBookmark );
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( TransientException.class ) );
                assertThat( e.getMessage(), startsWith( "Database not up to the requested version" ) );
            }
        }
    }

    @Test
    public void shouldHandleGracefulLeaderSwitch() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            Session session1 = driver.session();
            Transaction tx1 = session1.beginTransaction();

            // gracefully stop current leader to force re-election
            cluster.stop( leader );

            tx1.run( "CREATE (person:Person {name: {name}, title: {title}})",
                    parameters( "name", "Webber", "title", "Mr" ) );
            tx1.success();

            closeAndExpectException( tx1, SessionExpiredException.class );
            session1.close();

            String bookmark = inExpirableSession( driver, createSession(), new Function<Session,String>()
            {
                @Override
                public String apply( Session session )
                {
                    try ( Transaction tx = session.beginTransaction() )
                    {
                        tx.run( "CREATE (person:Person {name: {name}, title: {title}})",
                                parameters( "name", "Webber", "title", "Mr" ) );
                        tx.success();
                    }
                    return session.lastBookmark();
                }
            } );

            try ( Session session2 = driver.session( AccessMode.READ, bookmark );
                  Transaction tx2 = session2.beginTransaction() )
            {
                Record record = tx2.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                tx2.success();
                assertEquals( 1, record.get( "count" ).asInt() );
            }
        }
    }

    @Test
    public void shouldNotServeWritesWhenMajorityOfCoresAreDead() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            for ( ClusterMember follower : cluster.followers() )
            {
                cluster.kill( follower );
            }
            awaitLeaderToStepDown( driver );

            // now we should be unable to write because majority of cores is down
            for ( int i = 0; i < 10; i++ )
            {
                try ( Session session = driver.session( AccessMode.WRITE ) )
                {
                    session.run( "CREATE (p:Person {name: 'Gamora'})" ).consume();
                    fail( "Exception expected" );
                }
                catch ( Exception e )
                {
                    assertThat( e, instanceOf( SessionExpiredException.class ) );
                }
            }
        }
    }

    @Test
    public void shouldServeReadsWhenMajorityOfCoresAreDead() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            String bookmark;
            try ( Session session = driver.session() )
            {
                int writeResult = session.writeTransaction( new TransactionWork<Integer>()
                {
                    @Override
                    public Integer execute( Transaction tx )
                    {
                        StatementResult result = tx.run( "CREATE (:Person {name: 'Star Lord'}) RETURN 42" );
                        return result.single().get( 0 ).asInt();
                    }
                } );

                assertEquals( 42, writeResult );
                bookmark = session.lastBookmark();
            }

            ensureNodeVisible( cluster, "Star Lord", bookmark );

            for ( ClusterMember follower : cluster.followers() )
            {
                cluster.kill( follower );
            }
            awaitLeaderToStepDown( driver );

            // now we should be unable to write because majority of cores is down
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                session.run( "CREATE (p:Person {name: 'Gamora'})" ).consume();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( SessionExpiredException.class ) );
            }

            // but we should be able to read from the remaining core or read replicas
            try ( Session session = driver.session() )
            {
                int count = session.readTransaction( new TransactionWork<Integer>()
                {
                    @Override
                    public Integer execute( Transaction tx )
                    {
                        StatementResult result = tx.run( "MATCH (:Person {name: 'Star Lord'}) RETURN count(*)" );
                        return result.single().get( 0 ).asInt();
                    }
                } );

                assertEquals( 1, count );
            }
        }
    }

    @Test
    public void shouldAcceptMultipleBookmarks() throws Exception
    {
        int threadCount = 5;
        String label = "Person";
        String property = "name";
        String value = "Alice";

        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();
        ExecutorService executor = Executors.newCachedThreadPool();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            List<Future<String>> futures = new ArrayList<>();
            for ( int i = 0; i < threadCount; i++ )
            {
                futures.add( executor.submit( createNodeAndGetBookmark( driver, label, property, value ) ) );
            }

            List<String> bookmarks = new ArrayList<>();
            for ( Future<String> future : futures )
            {
                bookmarks.add( future.get( 10, SECONDS ) );
            }

            executor.shutdown();
            assertTrue( executor.awaitTermination( 5, SECONDS ) );

            try ( Session session = driver.session( AccessMode.READ, bookmarks ) )
            {
                int count = countNodes( session, label, property, value );
                assertEquals( count, threadCount );
            }
        }
    }

    private int executeWriteAndReadThroughBolt( ClusterMember member ) throws TimeoutException, InterruptedException
    {
        try ( Driver driver = createDriver( member.getRoutingUri() ) )
        {
            return inExpirableSession( driver, createWritableSession( null ), executeWriteAndRead() );
        }
    }

    private int executeWriteAndReadThroughBoltOnFirstAvailableAddress( ClusterMember... members ) throws TimeoutException, InterruptedException
    {
        List<URI> addresses = new ArrayList<>( members.length );
        for ( ClusterMember member : members )
        {
            addresses.add( member.getRoutingUri() );
        }
        try ( Driver driver = discoverDriver( addresses ) )
        {
            return inExpirableSession( driver, createWritableSession( null ), executeWriteAndRead() );
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

    private Function<Driver,Session> createWritableSession( final String bookmark )
    {
        return new Function<Driver,Session>()
        {
            @Override
            public Session apply( Driver driver )
            {
                return driver.session( AccessMode.WRITE, bookmark );
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
            catch ( SessionExpiredException | ServiceUnavailableException e )
            {
                // role might have changed; try again;
            }

            Thread.sleep( 500 );
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }

    private void ensureNodeVisible( Cluster cluster, String name, String bookmark )
    {
        for ( ClusterMember member : cluster.members() )
        {
            int count = countNodesUsingDirectDriver( member.getBoltUri(), name, bookmark );
            assertEquals( 1, count );
        }
    }

    private int countNodesUsingDirectDriver( URI boltUri, final String name, String bookmark )
    {
        try ( Driver driver = createDriver( boltUri );
              Session session = driver.session( bookmark ) )
        {
            return session.readTransaction( new TransactionWork<Integer>()
            {
                @Override
                public Integer execute( Transaction tx )
                {
                    StatementResult result = tx.run( "MATCH (:Person {name: {name}}) RETURN count(*)",
                            parameters( "name", name ) );
                    return result.single().get( 0 ).asInt();
                }
            } );
        }
    }

    private void awaitLeaderToStepDown( Driver driver )
    {
        int leadersCount;
        int followersCount;
        int readReplicasCount;
        do
        {
            try ( Session session = driver.session() )
            {
                int newLeadersCount = 0;
                int newFollowersCount = 0;
                int newReadReplicasCount = 0;
                for ( Record record : session.run( "CALL dbms.cluster.overview()" ).list() )
                {
                    ClusterMemberRole role = ClusterMemberRole.valueOf( record.get( "role" ).asString() );
                    if ( role == ClusterMemberRole.LEADER )
                    {
                        newLeadersCount++;
                    }
                    else if ( role == ClusterMemberRole.FOLLOWER )
                    {
                        newFollowersCount++;
                    }
                    else if ( role == ClusterMemberRole.READ_REPLICA )
                    {
                        newReadReplicasCount++;
                    }
                    else
                    {
                        throw new AssertionError( "Unknown role: " + role );
                    }
                }
                leadersCount = newLeadersCount;
                followersCount = newFollowersCount;
                readReplicasCount = newReadReplicasCount;
            }
        }
        while ( !(leadersCount == 0 && followersCount == 1 && readReplicasCount == 2) );
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

    private Driver discoverDriver( List<URI> routingUris )
    {
        Config config = Config.build()
                .withLogging( DEV_NULL_LOGGING )
                .toConfig();

        return GraphDatabase.routingDriver( routingUris, clusterRule.getDefaultAuthToken(), config );
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

    private static void closeAndExpectException( AutoCloseable closeable, Class<? extends Exception> exceptionClass )
    {
        try
        {
            closeable.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( exceptionClass ) );
        }
    }

    private static int countNodes( Session session, final String label, final String property, final String value )
    {
        return session.readTransaction( new TransactionWork<Integer>()
        {
            @Override
            public Integer execute( Transaction tx )
            {
                StatementResult result = tx.run( "MATCH (n:" + label + " {" + property + ": $value}) RETURN count(n)",
                        parameters( "value", value ) );
                return result.single().get( 0 ).asInt();
            }
        } );
    }

    private static Callable<String> createNodeAndGetBookmark( final Driver driver, final String label,
            final String property, final String value )
    {
        return new Callable<String>()
        {
            @Override
            public String call()
            {
                try ( Session session = driver.session() )
                {
                    session.writeTransaction( new TransactionWork<Void>()
                    {
                        @Override
                        public Void execute( Transaction tx )
                        {
                            tx.run( "CREATE (n:" + label + ") SET n." + property + " = $value",
                                    parameters( "value", value ) );
                            return null;
                        }
                    } );
                    return session.lastBookmark();
                }
            }
        };
    }
}
