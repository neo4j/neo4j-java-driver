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

import org.junit.After;
import org.junit.AfterClass;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.ThrowingConnection;
import org.neo4j.driver.internal.util.ThrowingConnectionDriverFactory;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementRunner;
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.DaemonThreadFactory.daemon;

public class CausalClusteringIT
{
    private static final long DEFAULT_TIMEOUT_MS = 120_000;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    private ExecutorService executor;

    @After
    public void tearDown()
    {
        if ( executor != null )
        {
            executor.shutdownNow();
        }
    }

    @AfterClass
    public static void stopSharedCluster()
    {
        ClusterRule.stopSharedCluster();
    }

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
    public void shouldDropBrokenOldConnections() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        int concurrentSessionsCount = 9;
        int livenessCheckTimeoutMinutes = 2;

        Config config = Config.build()
                .withConnectionLivenessCheckTimeout( livenessCheckTimeoutMinutes, MINUTES )
                .withoutEncryption()
                .toConfig();

        FakeClock clock = new FakeClock();
        ThrowingConnectionDriverFactory driverFactory = new ThrowingConnectionDriverFactory( clock );

        URI routingUri = cluster.leader().getRoutingUri();
        AuthToken auth = clusterRule.getDefaultAuthToken();
        RetrySettings retrySettings = RetrySettings.DEFAULT;

        try ( Driver driver = driverFactory.newInstance( routingUri, auth, defaultRoutingSettings(), retrySettings, config ) )
        {
            // create nodes in different threads using different sessions and connections
            createNodesInDifferentThreads( concurrentSessionsCount, driver );

            // now pool contains many connections, make them all invalid
            List<ThrowingConnection> oldConnections = driverFactory.pollConnections();
            for ( ThrowingConnection oldConnection : oldConnections )
            {
                oldConnection.setNextResetError( new ServiceUnavailableException( "Unable to reset" ) );
            }

            // move clock forward more than configured liveness check timeout
            clock.progress( MINUTES.toMillis( livenessCheckTimeoutMinutes + 1 ) );

            // now all idle connections should be considered too old and will be verified during acquisition
            // they will appear broken because they were closed and new valid connection will be created
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                List<Record> records = session.run( "MATCH (n) RETURN count(n)" ).list();
                assertEquals( 1, records.size() );
                assertEquals( concurrentSessionsCount, records.get( 0 ).get( 0 ).asInt() );
            }

            // all old connections failed to reset and should be closed
            for ( ThrowingConnection connection : oldConnections )
            {
                assertFalse( connection.isOpen() );
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
                // todo: configure bookmark wait timeout to be lower than default 30sec when neo4j supports this
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
        executor = newExecutor();

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
                assertEquals( threadCount, count );
            }
        }
    }

    @Test
    public void shouldAllowExistingTransactionToCompleteAfterDifferentConnectionBreaks()
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        ThrowingConnectionDriverFactory driverFactory = new ThrowingConnectionDriverFactory();
        try ( Driver driver = driverFactory.newInstance( leader.getRoutingUri(), clusterRule.getDefaultAuthToken(),
                defaultRoutingSettings(), RetrySettings.DEFAULT, configWithoutLogging() ) )
        {
            Session session1 = driver.session();
            Transaction tx1 = session1.beginTransaction();
            tx1.run( "CREATE (n:Node1 {name: 'Node1'})" ).consume();

            Session session2 = driver.session();
            Transaction tx2 = session2.beginTransaction();
            tx2.run( "CREATE (n:Node2 {name: 'Node2'})" ).consume();

            ServiceUnavailableException error = new ServiceUnavailableException( "Connection broke!" );
            setupLastConnectionToThrow( driverFactory, error );
            assertUnableToRunMoreStatementsInTx( tx2, error );

            closeTx( tx2 );
            closeTx( tx1 );

            try ( Session session3 = driver.session( session1.lastBookmark() ) )
            {
                // tx1 should not be terminated and should commit successfully
                assertEquals( 1, countNodes( session3, "Node1", "name", "Node1" ) );
                // tx2 should not commit because of a connection failure
                assertEquals( 0, countNodes( session3, "Node2", "name", "Node2" ) );
            }

            // rediscovery should happen for the new write query
            String session4Bookmark = createNodeAndGetBookmark( driver.session(), "Node3", "name", "Node3" );
            try ( Session session5 = driver.session( session4Bookmark ) )
            {
                assertEquals( 1, countNodes( session5, "Node3", "name", "Node3" ) );
            }
        }
    }

    @Test
    public void shouldRediscoverWhenConnectionsToAllCoresBreak()
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        ThrowingConnectionDriverFactory driverFactory = new ThrowingConnectionDriverFactory();
        try ( Driver driver = driverFactory.newInstance( leader.getRoutingUri(), clusterRule.getDefaultAuthToken(),
                defaultRoutingSettings(), RetrySettings.DEFAULT, configWithoutLogging() ) )
        {
            try ( Session session = driver.session() )
            {
                createNode( session, "Person", "name", "Vision" );

                // force driver to connect to every cluster member
                for ( int i = 0; i < cluster.members().size(); i++ )
                {
                    assertEquals( 1, countNodes( session, "Person", "name", "Vision" ) );
                }
            }

            // now driver should have connection pools towards every cluster member
            // make all those connections throw and seem broken
            for ( ThrowingConnection connection : driverFactory.getConnections() )
            {
                connection.setNextRunError( new ServiceUnavailableException( "Disconnected" ) );
            }

            // observe that connection towards writer is broken
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                try
                {
                    runCreateNode( session, "Person", "name", "Vision" );
                    fail( "Exception expected" );
                }
                catch ( SessionExpiredException e )
                {
                    assertEquals( "Disconnected", e.getCause().getMessage() );
                }
            }

            // probe connections to all readers
            int readersCount = cluster.followers().size() + cluster.readReplicas().size();
            for ( int i = 0; i < readersCount; i++ )
            {
                try ( Session session = driver.session( AccessMode.READ ) )
                {
                    runCountNodes( session, "Person", "name", "Vision" );
                }
                catch ( Throwable ignore )
                {
                }
            }

            try ( Session session = driver.session() )
            {
                updateNode( session, "Person", "name", "Vision", "Thanos" );
                assertEquals( 0, countNodes( session, "Person", "name", "Vision" ) );
                assertEquals( 1, countNodes( session, "Person", "name", "Thanos" ) );
            }
        }
    }

    @Test
    public void shouldKeepOperatingWhenConnectionsBreak() throws Exception
    {
        long testRunTimeMs = MINUTES.toMillis( 1 );
        String label = "Person";
        String property = "name";
        String value = "Tony Stark";
        Cluster cluster = clusterRule.getCluster();

        ThrowingConnectionDriverFactory driverFactory = new ThrowingConnectionDriverFactory();
        AtomicBoolean stop = new AtomicBoolean();
        executor = newExecutor();

        Config config = Config.build()
                .withLogging( DEV_NULL_LOGGING )
                .withMaxTransactionRetryTime( testRunTimeMs, MILLISECONDS )
                .toConfig();

        try ( Driver driver = driverFactory.newInstance( cluster.leader().getRoutingUri(), clusterRule.getDefaultAuthToken(),
                defaultRoutingSettings(), RetrySettings.DEFAULT, config ) )
        {
            List<Future<?>> results = new ArrayList<>();

            // launch writers and readers that use transaction functions and thus should never fail
            for ( int i = 0; i < 3; i++ )
            {
                results.add( executor.submit( readNodesCallable( driver, label, property, value, stop ) ) );
            }
            for ( int i = 0; i < 2; i++ )
            {
                results.add( executor.submit( createNodesCallable( driver, label, property, value, stop ) ) );
            }

            // make connections throw while reads and writes are in progress
            long deadline = System.currentTimeMillis() + MINUTES.toMillis( 1 );
            while ( System.currentTimeMillis() < deadline && !stop.get() )
            {
                List<ThrowingConnection> connections = driverFactory.pollConnections();
                for ( ThrowingConnection connection : connections )
                {
                    connection.setNextRunError( new ServiceUnavailableException( "Unable to execute query" ) );
                }
                SECONDS.sleep( 10 ); // sleep a bit to allow readers and writers to progress
            }
            stop.set( true );

            awaitAll( results ); // readers and writers should stop
            assertThat( countNodes( driver.session(), label, property, value ), greaterThan( 0 ) ); // some nodes should be created
        }
    }

    private static void closeTx( Transaction tx )
    {
        tx.success();
        tx.close();
    }

    private static void assertUnableToRunMoreStatementsInTx( Transaction tx, ServiceUnavailableException cause )
    {
        try
        {
            tx.run( "CREATE (n:Node3 {name: 'Node3'})" ).consume();
            fail( "Exception expected" );
        }
        catch ( SessionExpiredException e )
        {
            assertEquals( cause, e.getCause() );
        }
    }

    private static void setupLastConnectionToThrow( ThrowingConnectionDriverFactory factory, RuntimeException error )
    {
        List<ThrowingConnection> connections = factory.getConnections();
        ThrowingConnection lastConnection = connections.get( connections.size() - 1 );
        lastConnection.setNextRunError( error );
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
        return GraphDatabase.driver( boltUri, clusterRule.getDefaultAuthToken(), configWithoutLogging() );
    }

    private Driver discoverDriver( List<URI> routingUris )
    {
        return GraphDatabase.routingDriver( routingUris, clusterRule.getDefaultAuthToken(), configWithoutLogging() );
    }

    private static void createNodesInDifferentThreads( int count, final Driver driver ) throws Exception
    {
        final CountDownLatch beforeRunLatch = new CountDownLatch( count );
        final CountDownLatch runQueryLatch = new CountDownLatch( 1 );
        final ExecutorService executor = newExecutor();

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
        assertTrue( executor.awaitTermination( 1, MINUTES ) );
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
                return runCountNodes( tx, label, property, value );
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
                return createNodeAndGetBookmark( driver.session(), label, property, value );
            }
        };
    }

    private static String createNodeAndGetBookmark( final Session session, final String label, final String property,
            final String value )
    {
        try ( Session localSession = session )
        {
            localSession.writeTransaction( new TransactionWork<Void>()
            {
                @Override
                public Void execute( Transaction tx )
                {
                    tx.run( "CREATE (n:" + label + ") SET n." + property + " = $value",
                            parameters( "value", value ) );
                    return null;
                }
            } );
            return localSession.lastBookmark();
        }
    }

    private static Callable<Void> createNodesCallable( final Driver driver, final String label, final String property, final String value,
            final AtomicBoolean stop )
    {
        return new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                while ( !stop.get() )
                {
                    try ( Session session = driver.session( AccessMode.WRITE ) )
                    {
                        createNode( session, label, property, value );
                    }
                    catch ( Throwable t )
                    {
                        stop.set( true );
                        throw t;
                    }
                }
                return null;
            }
        };
    }

    private static Callable<Void> readNodesCallable( final Driver driver, final String label, final String property, final String value,
            final AtomicBoolean stop )
    {
        return new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                while ( !stop.get() )
                {
                    try ( Session session = driver.session( AccessMode.READ ) )
                    {
                        readNodeIds( session, label, property, value );
                    }
                    catch ( Throwable t )
                    {
                        stop.set( true );
                        throw t;
                    }
                }
                return null;
            }
        };
    }

    private static void createNode( final Session session, final String label, final String property, final String value )
    {
        session.writeTransaction( new TransactionWork<Void>()
        {
            @Override
            public Void execute( Transaction tx )
            {
                runCreateNode( tx, label, property, value );
                return null;
            }
        } );
    }

    private static List<Long> readNodeIds( final Session session, final String label, final String property, final String value )
    {
        return session.readTransaction( new TransactionWork<List<Long>>()
        {
            @Override
            public List<Long> execute( Transaction tx )
            {
                StatementResult result = tx.run( "MATCH (n:" + label + " {" + property + ": $value}) RETURN n LIMIT 5",
                        parameters( "value", value ) );

                List<Long> ids = new ArrayList<>();
                while ( result.hasNext() )
                {
                    ids.add( result.next().get( 0 ).asNode().id() );
                }
                return ids;
            }
        } );
    }

    private static void updateNode( final Session session, final String label, final String property, final String oldValue, final String newValue )
    {
        session.writeTransaction( new TransactionWork<Void>()
        {
            @Override
            public Void execute( Transaction tx )
            {
                tx.run( "MATCH (n: " + label + '{' + property + ": $oldValue}) SET n." + property + " = $newValue",
                        parameters( "oldValue", oldValue, "newValue", newValue ) );
                return null;
            }
        } );
    }

    private static void runCreateNode( StatementRunner statementRunner, String label, String property, String value )
    {
        statementRunner.run( "CREATE (n:" + label + ") SET n." + property + " = $value", parameters( "value", value ) );
    }

    private static int runCountNodes( StatementRunner statementRunner, String label, String property, String value )
    {
        StatementResult result = statementRunner.run( "MATCH (n:" + label + " {" + property + ": $value}) RETURN count(n)", parameters( "value", value ) );
        return result.single().get( 0 ).asInt();
    }

    private static RoutingSettings defaultRoutingSettings()
    {
        return new RoutingSettings( 1, SECONDS.toMillis( 1 ), null );
    }

    private static Config configWithoutLogging()
    {
        return Config.build().withLogging( DEV_NULL_LOGGING ).toConfig();
    }

    private static ExecutorService newExecutor()
    {
        return Executors.newCachedThreadPool( daemon( CausalClusteringIT.class.getSimpleName() + "-thread-" ) );
    }

    private static void awaitAll( List<Future<?>> results ) throws Exception
    {
        for ( Future<?> result : results )
        {
            assertNull( result.get( DEFAULT_TIMEOUT_MS, MILLISECONDS ) );
        }
    }
}
