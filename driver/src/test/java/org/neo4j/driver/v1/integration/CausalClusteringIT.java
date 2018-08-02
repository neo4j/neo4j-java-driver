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

import io.netty.channel.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.ChannelTrackingDriverFactory;
import org.neo4j.driver.internal.util.FailingConnectionDriverFactory;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.internal.util.ThrowingMessageEncoder;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.StatementRunner;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.cc.Cluster;
import org.neo4j.driver.v1.util.cc.ClusterExtension;
import org.neo4j.driver.v1.util.cc.ClusterMember;
import org.neo4j.driver.v1.util.cc.ClusterMemberRole;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Matchers.connectionAcquisitionTimeoutError;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V3;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.DaemonThreadFactory.daemon;
import static org.neo4j.driver.v1.util.TestUtil.await;
import static org.neo4j.driver.v1.util.TestUtil.awaitAllFutures;

class CausalClusteringIT
{
    private static final long DEFAULT_TIMEOUT_MS = 120_000;

    @RegisterExtension
    static final ClusterExtension clusterRule = new ClusterExtension();

    private ExecutorService executor;

    @AfterEach
    void tearDown()
    {
        if ( executor != null )
        {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfLeader() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        int count = executeWriteAndReadThroughBolt( cluster.leader() );

        assertEquals( 1, count );
    }

    @Test
    void shouldExecuteReadAndWritesWhenRouterIsDiscovered() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        int count = executeWriteAndReadThroughBoltOnFirstAvailableAddress( cluster.anyReadReplica(), cluster.leader() );

        assertEquals( 1, count );
    }

    @Test
    void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfFollower() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        int count = executeWriteAndReadThroughBolt( cluster.anyFollower() );

        assertEquals( 1, count );
    }

    @Test
    void sessionCreationShouldFailIfCallingDiscoveryProcedureOnEdgeServer()
    {
        Cluster cluster = clusterRule.getCluster();

        ClusterMember readReplica = cluster.anyReadReplica();
        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> createDriver( readReplica.getRoutingUri() ) );
        assertThat( e.getMessage(), containsString( "Failed to run 'CALL dbms.cluster.routing" ) );
    }

    // Ensure that Bookmarks work with single instances using a driver created using a bolt[not+routing] URI.
    @Test
    void bookmarksShouldWorkWithDriverPinnedToSingleServer() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getBoltUri() ) )
        {
            String bookmark = inExpirableSession( driver, Driver::session, session ->
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                    tx.success();
                }

                return session.lastBookmark();
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
    void shouldUseBookmarkFromAReadSessionInAWriteSession() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getBoltUri() ) )
        {
            inExpirableSession( driver, createWritableSession( null ), session ->
            {
                session.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                return null;
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

            inExpirableSession( driver, createWritableSession( bookmark ), session ->
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                    tx.success();
                }

                return null;
            } );

            try ( Session session = driver.session() )
            {
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 2, record.get( "count" ).asInt() );
            }
        }
    }

    @Test
    void shouldDropBrokenOldConnections() throws Exception
    {
        Cluster cluster = clusterRule.getCluster();

        int concurrentSessionsCount = 9;
        int livenessCheckTimeoutMinutes = 2;

        Config config = Config.build()
                .withConnectionLivenessCheckTimeout( livenessCheckTimeoutMinutes, MINUTES )
                .withLogging( DEV_NULL_LOGGING )
                .toConfig();

        FakeClock clock = new FakeClock();
        ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( clock );

        URI routingUri = cluster.leader().getRoutingUri();
        AuthToken auth = clusterRule.getDefaultAuthToken();
        RetrySettings retrySettings = RetrySettings.DEFAULT;

        try ( Driver driver = driverFactory.newInstance( routingUri, auth, defaultRoutingSettings(), retrySettings, config ) )
        {
            // create nodes in different threads using different sessions and connections
            createNodesInDifferentThreads( concurrentSessionsCount, driver );

            // now pool contains many channels, make them all invalid
            List<Channel> oldChannels = driverFactory.channels();
            for ( Channel oldChannel : oldChannels )
            {
                RuntimeException error = new ServiceUnavailableException( "Unable to reset" );
                oldChannel.pipeline().addLast( ThrowingMessageEncoder.forResetMessage( error ) );
            }

            // move clock forward more than configured liveness check timeout
            clock.progress( MINUTES.toMillis( livenessCheckTimeoutMinutes + 1 ) );

            // now all idle channels should be considered too old and will be verified during acquisition
            // they will appear broken because they were closed and new valid connection will be created
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                List<Record> records = session.run( "MATCH (n) RETURN count(n)" ).list();
                assertEquals( 1, records.size() );
                assertEquals( concurrentSessionsCount, records.get( 0 ).get( 0 ).asInt() );
            }

            // all old channels failed to reset and should be closed
            for ( Channel oldChannel : oldChannels )
            {
                assertFalse( oldChannel.isActive() );
            }
        }
    }

    @Test
    void beginTransactionThrowsForInvalidBookmark()
    {
        String invalidBookmark = "hi, this is an invalid bookmark";
        ClusterMember leader = clusterRule.getCluster().leader();

        try ( Driver driver = createDriver( leader.getBoltUri() );
              Session session = driver.session( invalidBookmark ) )
        {
            ClientException e = assertThrows( ClientException.class, session::beginTransaction );
            assertThat( e.getMessage(), containsString( invalidBookmark ) );
        }
    }

    @SuppressWarnings( "deprecation" )
    @Test
    void beginTransactionThrowsForUnreachableBookmark()
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

            TransientException e = assertThrows( TransientException.class, () -> session.beginTransaction( newBookmark ) );
            assertThat( e.getMessage(), startsWith( "Database not up to the requested version" ) );
        }
    }

    @Test
    void shouldHandleGracefulLeaderSwitch() throws Exception
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

            assertThrows( (Class<? extends Exception>) SessionExpiredException.class, ((AutoCloseable) tx1)::close );
            session1.close();

            String bookmark = inExpirableSession( driver, Driver::session, session ->
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (person:Person {name: {name}, title: {title}})",
                            parameters( "name", "Webber", "title", "Mr" ) );
                    tx.success();
                }
                return session.lastBookmark();
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
    void shouldNotServeWritesWhenMajorityOfCoresAreDead()
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            Set<ClusterMember> cores = cluster.cores();
            for ( ClusterMember follower : cluster.followers() )
            {
                cluster.kill( follower );
            }
            awaitLeaderToStepDown( cores );

            // now we should be unable to write because majority of cores is down
            for ( int i = 0; i < 10; i++ )
            {
                assertThrows( SessionExpiredException.class, () ->
                {
                    try ( Session session = driver.session( AccessMode.WRITE ) )
                    {
                        session.run( "CREATE (p:Person {name: 'Gamora'})" ).consume();
                    }
                } );
            }
        }
    }

    @Test
    void shouldServeReadsWhenMajorityOfCoresAreDead()
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            String bookmark;
            try ( Session session = driver.session() )
            {
                int writeResult = session.writeTransaction( tx ->
                {
                    StatementResult result = tx.run( "CREATE (:Person {name: 'Star Lord'}) RETURN 42" );
                    return result.single().get( 0 ).asInt();
                } );

                assertEquals( 42, writeResult );
                bookmark = session.lastBookmark();
            }

            ensureNodeVisible( cluster, "Star Lord", bookmark );

            Set<ClusterMember> cores = cluster.cores();
            for ( ClusterMember follower : cluster.followers() )
            {
                cluster.kill( follower );
            }
            awaitLeaderToStepDown( cores );

            // now we should be unable to write because majority of cores is down
            assertThrows( SessionExpiredException.class, () ->
            {
                try ( Session session = driver.session( AccessMode.WRITE ) )
                {
                    session.run( "CREATE (p:Person {name: 'Gamora'})" ).consume();
                }
            } );

            // but we should be able to read from the remaining core or read replicas
            try ( Session session = driver.session() )
            {
                int count = session.readTransaction( tx ->
                {
                    StatementResult result = tx.run( "MATCH (:Person {name: 'Star Lord'}) RETURN COUNT(*)" );
                    return result.single().get( 0 ).asInt();
                } );

                assertEquals( 1, count );
            }
        }
    }

    @Test
    void shouldAcceptMultipleBookmarks() throws Exception
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
                assertEquals( count, threadCount );
            }
        }
    }

    @Test
    void shouldNotReuseReadConnectionForWriteTransaction()
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        try ( Driver driver = createDriver( leader.getRoutingUri() ) )
        {
            Session session = driver.session( AccessMode.READ );

            CompletionStage<List<RecordAndSummary>> resultsStage = session.runAsync( "RETURN 42" )
                    .thenCompose( cursor1 ->
                            session.writeTransactionAsync( tx -> tx.runAsync( "CREATE (:Node1) RETURN 42" ) )
                                    .thenCompose( cursor2 -> combineCursors( cursor2, cursor1 ) ) );

            List<RecordAndSummary> results = await( resultsStage );
            assertEquals( 2, results.size() );

            RecordAndSummary first = results.get( 0 );
            RecordAndSummary second = results.get( 1 );

            // both auto-commit query and write tx should return 42
            assertEquals( 42, first.record.get( 0 ).asInt() );
            assertEquals( first.record, second.record );
            // they should not use same server
            assertNotEquals( first.summary.server().address(), second.summary.server().address() );

            CompletionStage<Integer> countStage =
                    session.readTransaction( tx -> tx.runAsync( "MATCH (n:Node1) RETURN count(n)" )
                            .thenCompose( StatementResultCursor::singleAsync ) )
                            .thenApply( record -> record.get( 0 ).asInt() );

            assertEquals( 1, await( countStage ).intValue() );

            await( session.closeAsync() );
        }
    }

    @Test
    void shouldRespectMaxConnectionPoolSizePerClusterMember()
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        Config config = Config.build()
                .withMaxConnectionPoolSize( 2 )
                .withConnectionAcquisitionTimeout( 42, MILLISECONDS )
                .withLogging( DEV_NULL_LOGGING )
                .toConfig();

        try ( Driver driver = createDriver( leader.getRoutingUri(), config ) )
        {
            Session writeSession1 = driver.session( AccessMode.WRITE );
            writeSession1.beginTransaction();

            Session writeSession2 = driver.session( AccessMode.WRITE );
            writeSession2.beginTransaction();

            // should not be possible to acquire more connections towards leader because limit is 2
            Session writeSession3 = driver.session( AccessMode.WRITE );
            ClientException e = assertThrows( ClientException.class, writeSession3::beginTransaction );
            assertThat( e, is( connectionAcquisitionTimeoutError( 42 ) ) );

            // should be possible to acquire new connection towards read server
            // it's a different machine, not leader, so different max connection pool size limit applies
            Session readSession = driver.session( AccessMode.READ );
            Record record = readSession.readTransaction( tx -> tx.run( "RETURN 1" ).single() );
            assertEquals( 1, record.get( 0 ).asInt() );
        }
    }

    @Test
    void shouldAllowExistingTransactionToCompleteAfterDifferentConnectionBreaks()
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        FailingConnectionDriverFactory driverFactory = new FailingConnectionDriverFactory();

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
            driverFactory.setNextRunFailure( error );
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
    void shouldRediscoverWhenConnectionsToAllCoresBreak()
    {
        Cluster cluster = clusterRule.getCluster();
        ClusterMember leader = cluster.leader();

        ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory();
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

            // now driver should have connections towards every cluster member
            // make all those connections throw and seem broken
            makeAllChannelsFailToRunQueries( driverFactory, ServerVersion.version( driver ) );

            // observe that connection towards writer is broken
            try ( Session session = driver.session( AccessMode.WRITE ) )
            {
                SessionExpiredException e = assertThrows( SessionExpiredException.class,
                        () -> runCreateNode( session, "Person", "name", "Vision" ).consume() );
                assertEquals( "Disconnected", e.getCause().getMessage() );
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
    void shouldKeepOperatingWhenConnectionsBreak() throws Exception
    {
        long testRunTimeMs = MINUTES.toMillis( 1 );
        String label = "Person";
        String property = "name";
        String value = "Tony Stark";
        Cluster cluster = clusterRule.getCluster();

        ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory();
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
                List<Channel> channels = driverFactory.pollChannels();
                for ( Channel channel : channels )
                {
                    RuntimeException error = new ServiceUnavailableException( "Unable to execute query" );
                    channel.pipeline().addLast( ThrowingMessageEncoder.forRunMessage( error ) );
                }
                SECONDS.sleep( 10 ); // sleep a bit to allow readers and writers to progress
            }
            stop.set( true );

            awaitAllFutures( results ); // readers and writers should stop
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
        SessionExpiredException e = assertThrows( SessionExpiredException.class, () -> tx.run( "CREATE (n:Node3 {name: 'Node3'})" ).consume() );
        assertEquals( cause, e.getCause() );
    }

    private CompletionStage<List<RecordAndSummary>> combineCursors( StatementResultCursor cursor1,
            StatementResultCursor cursor2 )
    {
        return buildRecordAndSummary( cursor1 ).thenCombine( buildRecordAndSummary( cursor2 ),
                ( rs1, rs2 ) -> Arrays.asList( rs1, rs2 ) );
    }

    private CompletionStage<RecordAndSummary> buildRecordAndSummary( StatementResultCursor cursor )
    {
        return cursor.singleAsync().thenCompose( record ->
                cursor.summaryAsync().thenApply( summary -> new RecordAndSummary( record, summary ) ) );
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

    private Function<Driver,Session> createWritableSession( final String bookmark )
    {
        return driver -> driver.session( AccessMode.WRITE, bookmark );
    }

    private Function<Session,Integer> executeWriteAndRead()
    {
        return session ->
        {
            session.run( "MERGE (n:Person {name: 'Jim'})" ).consume();
            Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
            return record.get( "count" ).asInt();
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
            int count = countNodesUsingDirectDriver( member, name, bookmark );
            assertEquals( 1, count );
        }
    }

    private int countNodesUsingDirectDriver( ClusterMember member, final String name, String bookmark )
    {
        Driver driver = clusterRule.getCluster().getDirectDriver( member );
        try ( Session session = driver.session( bookmark ) )
        {
            return session.readTransaction( tx ->
            {
                StatementResult result = tx.run( "MATCH (:Person {name: {name}}) RETURN count(*)",
                        parameters( "name", name ) );
                return result.single().get( 0 ).asInt();
            } );
        }
    }

    private void awaitLeaderToStepDown( Set<ClusterMember> cores )
    {
        long deadline = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;
        ClusterOverview overview = null;
        do
        {
            for ( ClusterMember core : cores )
            {
                overview = fetchClusterOverview( core );
                if ( overview != null )
                {
                    break;
                }
            }
        }
        while ( !isSingleFollowerWithReadReplicas( overview ) && System.currentTimeMillis() <= deadline );

        if ( System.currentTimeMillis() > deadline )
        {
            throw new IllegalStateException( "Leader did not step down in " + DEFAULT_TIMEOUT_MS + "ms. Last seen cluster overview: " + overview );
        }
    }

    private Driver createDriver( URI boltUri )
    {
        return createDriver( boltUri, configWithoutLogging() );
    }

    private Driver createDriver( URI boltUri, Config config )
    {
        return GraphDatabase.driver( boltUri, clusterRule.getDefaultAuthToken(), config );
    }

    private Driver discoverDriver( List<URI> routingUris )
    {
        return GraphDatabase.routingDriver( routingUris, clusterRule.getDefaultAuthToken(), configWithoutLogging() );
    }

    private ClusterOverview fetchClusterOverview( ClusterMember member )
    {
        int leaderCount = 0;
        int followerCount = 0;
        int readReplicaCount = 0;

        Driver driver = clusterRule.getCluster().getDirectDriver( member );
        try ( Session session = driver.session() )
        {
            for ( Record record : session.run( "CALL dbms.cluster.overview()" ).list() )
            {
                ClusterMemberRole role = ClusterMemberRole.valueOf( record.get( "role" ).asString() );
                if ( role == ClusterMemberRole.LEADER )
                {
                    leaderCount++;
                }
                else if ( role == ClusterMemberRole.FOLLOWER )
                {
                    followerCount++;
                }
                else if ( role == ClusterMemberRole.READ_REPLICA )
                {
                    readReplicaCount++;
                }
                else
                {
                    throw new AssertionError( "Unknown role: " + role );
                }
            }
            return new ClusterOverview( leaderCount, followerCount, readReplicaCount );
        }
        catch ( Neo4jException ignore )
        {
            return null;
        }
    }

    private static void createNodesInDifferentThreads( int count, final Driver driver ) throws Exception
    {
        final CountDownLatch beforeRunLatch = new CountDownLatch( count );
        final CountDownLatch runQueryLatch = new CountDownLatch( 1 );
        final ExecutorService executor = newExecutor();

        for ( int i = 0; i < count; i++ )
        {
            executor.submit( () ->
            {
                beforeRunLatch.countDown();
                try ( Session session = driver.session( AccessMode.WRITE ) )
                {
                    runQueryLatch.await();
                    session.run( "CREATE ()" );
                }
                return null;
            } );
        }

        beforeRunLatch.await();
        runQueryLatch.countDown();

        executor.shutdown();
        assertTrue( executor.awaitTermination( 1, TimeUnit.MINUTES ) );
    }

    private static Callable<Void> createNodesCallable( Driver driver, String label, String property, String value, AtomicBoolean stop )
    {
        return () ->
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
        };
    }

    private static Callable<Void> readNodesCallable( Driver driver, String label, String property, String value, AtomicBoolean stop )
    {
        return () ->
        {
            while ( !stop.get() )
            {
                try ( Session session = driver.session( AccessMode.READ ) )
                {
                    List<Long> ids = readNodeIds( session, label, property, value );
                    assertNotNull( ids );
                }
                catch ( Throwable t )
                {
                    stop.set( true );
                    throw t;
                }
            }
            return null;
        };
    }

    private static List<Long> readNodeIds( final Session session, final String label, final String property, final String value )
    {
        return session.readTransaction( tx ->
        {
            StatementResult result = tx.run( "MATCH (n:" + label + " {" + property + ": $value}) RETURN n LIMIT 10",
                    parameters( "value", value ) );

            return result.list( record -> record.get( 0 ).asNode().id() );
        } );
    }

    private static void createNode( Session session, String label, String property, String value )
    {
        session.writeTransaction( tx ->
        {
            runCreateNode( tx, label, property, value );
            return null;
        } );
    }

    private static void updateNode( Session session, String label, String property, String oldValue, String newValue )
    {
        session.writeTransaction( tx ->
        {
            tx.run( "MATCH (n: " + label + '{' + property + ": $oldValue}) SET n." + property + " = $newValue",
                    parameters( "oldValue", oldValue, "newValue", newValue ) );
            return null;
        } );
    }

    private static int countNodes( Session session, String label, String property, String value )
    {
        return session.readTransaction( tx -> runCountNodes( tx, label, property, value ) );
    }

    private static Callable<String> createNodeAndGetBookmark( Driver driver, String label, String property,
            String value )
    {
        return () -> createNodeAndGetBookmark( driver.session(), label, property, value );
    }

    private static String createNodeAndGetBookmark( Session session, String label, String property, String value )
    {
        try ( Session localSession = session )
        {
            localSession.writeTransaction( tx ->
            {
                runCreateNode( tx, label, property, value );
                return null;
            } );
            return localSession.lastBookmark();
        }
    }

    private static StatementResult runCreateNode( StatementRunner statementRunner, String label, String property, String value )
    {
        return statementRunner.run( "CREATE (n:" + label + ") SET n." + property + " = $value", parameters( "value", value ) );
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

    private static boolean isSingleFollowerWithReadReplicas( ClusterOverview overview )
    {
        if ( overview == null )
        {
            return false;
        }
        return overview.leaderCount == 0 &&
               overview.followerCount == 1 &&
               overview.readReplicaCount == ClusterExtension.READ_REPLICA_COUNT;
    }

    private static void makeAllChannelsFailToRunQueries( ChannelTrackingDriverFactory driverFactory, ServerVersion dbVersion )
    {
        for ( Channel channel : driverFactory.channels() )
        {
            RuntimeException error = new ServiceUnavailableException( "Disconnected" );
            if ( BOLT_V3.availableIn( dbVersion ) )
            {
                channel.pipeline().addLast( ThrowingMessageEncoder.forRunWithMetadataMessage( error ) );
            }
            else
            {
                channel.pipeline().addLast( ThrowingMessageEncoder.forRunMessage( error ) );
            }
        }
    }

    private static class RecordAndSummary
    {
        final Record record;
        final ResultSummary summary;

        RecordAndSummary( Record record, ResultSummary summary )
        {
            this.record = record;
            this.summary = summary;
        }
    }

    private static class ClusterOverview
    {
        final int leaderCount;
        final int followerCount;
        final int readReplicaCount;

        ClusterOverview( int leaderCount, int followerCount, int readReplicaCount )
        {
            this.leaderCount = leaderCount;
            this.followerCount = followerCount;
            this.readReplicaCount = readReplicaCount;
        }

        @Override
        public String toString()
        {
            return "ClusterOverview{" +
                   "leaderCount=" + leaderCount +
                   ", followerCount=" + followerCount +
                   ", readReplicaCount=" + readReplicaCount +
                   '}';
        }
    }
}
