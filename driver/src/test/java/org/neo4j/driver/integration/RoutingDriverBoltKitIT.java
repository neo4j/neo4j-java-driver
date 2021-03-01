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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.DriverFactoryWithClock;
import org.neo4j.driver.internal.util.DriverFactoryWithFixedRetryLogic;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.SleeplessClock;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.util.StubServer;
import org.neo4j.driver.util.StubServerController;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.internal.InternalBookmark.parse;
import static org.neo4j.driver.util.StubServer.INSECURE_CONFIG;
import static org.neo4j.driver.util.StubServer.insecureBuilder;

class RoutingDriverBoltKitIT
{
    private static StubServerController stubController;

    @BeforeAll
    public static void setup()
    {
        stubController = new StubServerController();
    }

    @AfterEach
    public void killServers()
    {
        stubController.reset();
    }

    @Test
    void shouldHandleLeaderSwitchAndRetryWhenWritingInTxFunction() throws IOException, InterruptedException
    {
        // Given
        StubServer server = stubController.startStub( "acquire_endpoints_twice_v4.script", 9001 );

        // START a write server that fails on the first write attempt but then succeeds on the second
        StubServer writeServer = stubController.startStub( "not_able_to_write_server_tx_func_retries.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );

        Driver driver = GraphDatabase.driver( uri, Config.builder().withMaxTransactionRetryTime( 1, TimeUnit.MILLISECONDS ).build() );
        List<String> names;

        try ( Session session = driver.session( builder().withDatabase( "mydatabase" ).build() ) )
        {
            names = session.writeTransaction( tx ->
            {
                tx.run( "RETURN 1" );
                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException ex )
                {
                }
                return tx.run( "MATCH (n) RETURN n.name" ).list( RoutingDriverBoltKitIT::extractNameField );
            } );
        }

        assertEquals( asList( "Foo", "Bar" ), names );

        // Finally
        driver.close();
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldHandleLeaderSwitchAndRetryWhenWritingInTxFunctionAsync() throws IOException, InterruptedException
    {
        // Given
        StubServer server = stubController.startStub( "acquire_endpoints_twice_v4.script", 9001 );

        // START a write server that fails on the first write attempt but then succeeds on the second
        StubServer writeServer = stubController.startStub( "not_able_to_write_server_tx_func_retries.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );

        Driver driver = GraphDatabase.driver( uri, Config.builder().withMaxTransactionRetryTime( 1, TimeUnit.MILLISECONDS ).build() );
        AsyncSession session = driver.asyncSession( builder().withDatabase( "mydatabase" ).build() );
        List<String> names = Futures.blockingGet( session.writeTransactionAsync(
                tx -> tx.runAsync( "RETURN 1" )
                        .thenComposeAsync( ignored -> {
                            try
                            {
                                Thread.sleep( 100 );
                            }
                            catch ( InterruptedException ex )
                            {
                            }
                            return tx.runAsync( "MATCH (n) RETURN n.name" );
                        } )
                        .thenComposeAsync( cursor -> cursor.listAsync( RoutingDriverBoltKitIT::extractNameField ) ) ) );

        assertEquals( asList( "Foo", "Bar" ), names );

        // Finally
        driver.close();
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer.exitStatus(), equalTo( 0 ) );
    }

    private static String extractNameField(Record record)
    {
        return record.get( 0 ).asString();
    }

    // This does not exactly reproduce the async and blocking versions above, as we don't have any means of ignoring
    // the flux of the RETURN 1 query (not pulling the result) like we do in above, so this is "just" a test for
    // a leader going away during the execution of a flux.
    @Test
    void shouldHandleLeaderSwitchAndRetryWhenWritingInTxFunctionRX() throws IOException, InterruptedException
    {
        // Given
        StubServer server = stubController.startStub( "acquire_endpoints_twice_v4.script", 9001 );

        // START a write server that fails on the first write attempt but then succeeds on the second
        StubServer writeServer = stubController.startStub( "not_able_to_write_server_tx_func_retries_rx.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );

        Driver driver = GraphDatabase.driver( uri, Config.builder().withMaxTransactionRetryTime( 1, TimeUnit.MILLISECONDS ).build() );

        Flux<String> fluxOfNames = Flux.usingWhen( Mono.fromSupplier( () -> driver.rxSession( builder().withDatabase( "mydatabase" ).build() ) ),
                session -> session.writeTransaction( tx ->
                {
                    RxResult result = tx.run( "RETURN 1" );
                    return Flux.from( result.records() ).limitRate( 100 ).thenMany( tx.run( "MATCH (n) RETURN n.name" ).records() ).limitRate( 100 ).map(
                            RoutingDriverBoltKitIT::extractNameField );
                } ), RxSession::close );

        StepVerifier.create( fluxOfNames ).expectNext( "Foo", "Bar" ).verifyComplete();

        // Finally
        driver.close();
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldSendInitialBookmark() throws Exception
    {
        StubServer router = stubController.startStub( "acquire_endpoints_v3.script", 9001 );
        StubServer writer = stubController.startStub( "write_tx_with_bookmarks.script", 9007 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9001", INSECURE_CONFIG );
                Session session = driver.session( builder().withBookmarks( parse( "OldBookmark" ) ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (n {name:'Bob'})" );
                tx.commit();
            }

            assertEquals( parse( "NewBookmark" ), session.lastBookmark() );
        }

        assertThat( router.exitStatus(), equalTo( 0 ) );
        assertThat( writer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldRetryWriteTransactionUntilSuccessWithWhenLeaderIsRemoved() throws Exception
    {
        // This test simulates a router in a cluster when a leader is removed.
        // The router first returns a RT with a writer inside.
        // However this writer is killed while the driver is running a tx with it.
        // Then at the second time the router returns the same RT with the killed writer inside.
        // At the third round, the router removes the the writer server from RT reply.
        // Finally, the router returns a RT with a reachable writer.
        StubServer router = stubController.startStub( "acquire_endpoints_v3_leader_killed.script", 9001 );
        StubServer brokenWriter = stubController.startStub( "dead_write_server.script", 9004 );
        StubServer writer = stubController.startStub( "write_server_v3_write_tx.script", 9008 );

        Logger logger = mock( Logger.class );
        Config config = insecureBuilder().withLogging( ignored -> logger ).build();
        try ( Driver driver = newDriverWithSleeplessClock( "neo4j://127.0.0.1:9001", config ); Session session = driver.session() )
        {
            AtomicInteger invocations = new AtomicInteger();
            List<Record> records = session.writeTransaction( queryWork( "CREATE (n {name:'Bob'})", invocations ) );

            assertEquals( 0, records.size() );
            assertEquals( 2, invocations.get() );
        }
        finally
        {
            assertEquals( 0, router.exitStatus() );
            assertEquals( 0, brokenWriter.exitStatus() );
            assertEquals( 0, writer.exitStatus() );
        }
        verify( logger, times( 3 ) ).warn( startsWith( "Transaction failed and will be retried in" ), any( SessionExpiredException.class ) );
        verify( logger ).warn( startsWith( "Failed to obtain a connection towards address 127.0.0.1:9004" ), any( SessionExpiredException.class ) );
    }

    @Test
    void shouldRetryWriteTransactionUntilSuccessWithWhenLeaderIsRemovedV3() throws Exception
    {
        // This test simulates a router in a cluster when a leader is removed.
        // The router first returns a RT with a writer inside.
        // However this writer is killed while the driver is running a tx with it.
        // Then at the second time the router returns the same RT with the killed writer inside.
        // At the third round, the router removes the the writer server from RT reply.
        // Finally, the router returns a RT with a reachable writer.
        StubServer router = stubController.startStub( "acquire_endpoints_v3_leader_killed.script", 9001 );
        StubServer brokenWriter = stubController.startStub( "database_shutdown_at_commit.script", 9004 );
        StubServer writer = stubController.startStub( "write_server_v3_write_tx.script", 9008 );

        Logger logger = mock( Logger.class );
        Config config = insecureBuilder().withLogging( ignored -> logger ).build();
        try ( Driver driver = newDriverWithSleeplessClock( "neo4j://127.0.0.1:9001", config ); Session session = driver.session() )
        {
            AtomicInteger invocations = new AtomicInteger();
            List<Record> records = session.writeTransaction( queryWork( "CREATE (n {name:'Bob'})", invocations ) );

            assertEquals( 0, records.size() );
            assertEquals( 2, invocations.get() );
        }
        finally
        {
            assertEquals( 0, router.exitStatus() );
            assertEquals( 0, brokenWriter.exitStatus() );
            assertEquals( 0, writer.exitStatus() );
        }
        verify( logger, times( 1 ) ).warn( startsWith( "Transaction failed and will be retried in" ), any( TransientException.class ) );
        verify( logger, times( 2 ) ).warn( startsWith( "Transaction failed and will be retried in" ), any( SessionExpiredException.class ) );
        verify( logger ).warn( startsWith( "Failed to obtain a connection towards address 127.0.0.1:9004" ), any( SessionExpiredException.class ) );
    }

    @Test
    void shouldRetryReadTransactionUntilFailure() throws Exception
    {
        StubServer router = stubController.startStub( "acquire_endpoints_v3.script", 9001 );
        StubServer brokenReader1 = stubController.startStub( "dead_read_server_tx.script", 9005 );
        StubServer brokenReader2 = stubController.startStub( "dead_read_server_tx.script", 9006 );

        try ( Driver driver = newDriverWithFixedRetries( "neo4j://127.0.0.1:9001", 1 ); Session session = driver.session() )
        {
            AtomicInteger invocations = new AtomicInteger();
            assertThrows( SessionExpiredException.class, () -> session.readTransaction( queryWork( "MATCH (n) RETURN n.name", invocations ) ) );
            assertEquals( 2, invocations.get() );
        }
        finally
        {
            assertEquals( 0, router.exitStatus() );
            assertEquals( 0, brokenReader1.exitStatus() );
            assertEquals( 0, brokenReader2.exitStatus() );
        }
    }

    @Test
    void shouldRetryWriteTransactionUntilFailure() throws Exception
    {
        StubServer router = stubController.startStub( "acquire_endpoints_v3.script", 9001 );
        StubServer brokenWriter1 = stubController.startStub( "dead_write_server.script", 9007 );
        StubServer brokenWriter2 = stubController.startStub( "dead_write_server.script", 9008 );

        try ( Driver driver = newDriverWithFixedRetries( "neo4j://127.0.0.1:9001", 1 ); Session session = driver.session() )
        {
            AtomicInteger invocations = new AtomicInteger();
            assertThrows( SessionExpiredException.class, () -> session.writeTransaction( queryWork( "CREATE (n {name:'Bob'})", invocations ) ) );
            assertEquals( 2, invocations.get() );
        }
        finally
        {
            assertEquals( 0, router.exitStatus() );
            assertEquals( 0, brokenWriter1.exitStatus() );
            assertEquals( 0, brokenWriter2.exitStatus() );
        }
    }

    @Test
    void shouldSendRoutingContextToServer() throws Exception
    {
        // stub server is both a router and reader
        StubServer server = stubController.startStub( "get_routing_table_with_context.script", 9001 );

        URI uri = URI.create( "neo4j://127.0.0.1:9001/?policy=my_policy&region=china" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ); Session session = driver.session() )
        {
            List<Record> records = session.run( "MATCH (n) RETURN n.name AS name" ).list();
            assertEquals( 2, records.size() );
            assertEquals( "Alice", records.get( 0 ).get( "name" ).asString() );
            assertEquals( "Bob", records.get( 1 ).get( "name" ).asString() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldSendRoutingContextInHelloMessage() throws Exception
    {
        // stub server is both a router and reader
        StubServer server = StubServer.start( "routing_context_in_hello_neo4j.script", 9001 );

        URI uri = URI.create( "neo4j://127.0.0.1:9001/?policy=my_policy&region=china" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ); Session session = driver.session() )
        {
            List<Record> records = session.run( "MATCH (n) RETURN n.name AS name" ).list();
            assertEquals( 2, records.size() );
            assertEquals( "Alice", records.get( 0 ).get( "name" ).asString() );
            assertEquals( "Bob", records.get( 1 ).get( "name" ).asString() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldFailInitialDiscoveryWhenConfiguredResolverThrows()
    {
        ServerAddressResolver resolver = mock( ServerAddressResolver.class );
        when( resolver.resolve( any( ServerAddress.class ) ) ).thenThrow( new RuntimeException( "Resolution failure!" ) );

        Config config = insecureBuilder().withResolver( resolver ).build();
        final Driver driver = GraphDatabase.driver( "neo4j://my.server.com:9001", config );

        RuntimeException error = assertThrows( RuntimeException.class, driver::verifyConnectivity );
        assertEquals( "Resolution failure!", error.getMessage() );
        verify( resolver ).resolve( ServerAddress.of( "my.server.com", 9001 ) );
    }

    @Test
    void useSessionAfterDriverIsClosed() throws Exception
    {
        StubServer router = stubController.startStub( "acquire_endpoints_v3.script", 9001 );
        StubServer readServer = stubController.startStub( "read_server_v3_read.script", 9005 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9001", INSECURE_CONFIG ) )
        {
            try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
            {
                List<Record> records = session.run( "MATCH (n) RETURN n.name" ).list();
                assertEquals( 3, records.size() );
            }

            Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() );

            driver.close();

            assertThrows( IllegalStateException.class, () -> session.run( "MATCH (n) RETURN n.name" ) );
        }
        finally
        {
            assertEquals( 0, readServer.exitStatus() );
            assertEquals( 0, router.exitStatus() );
        }
    }

    @Test
    void shouldServerWithBoltV4SupportMultiDb() throws Throwable
    {
        StubServer server = stubController.startStub( "support_multidb_v4.script", 9001 );
        try ( Driver driver = GraphDatabase.driver( "neo4j://localhost:9001", INSECURE_CONFIG ) )
        {
            assertTrue( driver.supportsMultiDb() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldServerWithBoltV3NotSupportMultiDb() throws Throwable
    {
        StubServer server = stubController.startStub( "support_multidb_v3.script", 9001 );
        try ( Driver driver = GraphDatabase.driver( "neo4j://localhost:9001", INSECURE_CONFIG ) )
        {
            assertFalse( driver.supportsMultiDb() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    private static Driver newDriverWithSleeplessClock( String uriString, Config config )
    {
        DriverFactory driverFactory = new DriverFactoryWithClock( new SleeplessClock() );
        return newDriver( uriString, driverFactory, config );
    }

    private static Driver newDriverWithSleeplessClock( String uriString )
    {
        return newDriverWithSleeplessClock( uriString, INSECURE_CONFIG );
    }

    private static Driver newDriverWithFixedRetries( String uriString, int retries )
    {
        DriverFactory driverFactory = new DriverFactoryWithFixedRetryLogic( retries );
        return newDriver( uriString, driverFactory, INSECURE_CONFIG );
    }

    private static Driver newDriver( String uriString, DriverFactory driverFactory, Config config )
    {
        URI uri = URI.create( uriString );
        RoutingSettings routingConf = new RoutingSettings( 1, 1, 0, null );
        AuthToken auth = AuthTokens.none();
        return driverFactory.newInstance( uri, auth, routingConf, RetrySettings.DEFAULT, config, SecurityPlanImpl.insecure() );
    }

    private static TransactionWork<List<Record>> queryWork( final String query, final AtomicInteger invocations )
    {
        return tx ->
        {
            invocations.incrementAndGet();
            return tx.run( query ).list();
        };
    }

    private static List<String> readStrings( final String query, Session session )
    {
        return session.readTransaction( tx ->
        {
            List<Record> records = tx.run( query ).list();
            List<String> names = new ArrayList<>( records.size() );
            for ( Record record : records )
            {
                names.add( record.get( 0 ).asString() );
            }
            return names;
        } );
    }

    static class PortBasedServerAddressComparator implements Comparator<ServerAddress>
    {
        @Override
        public int compare( ServerAddress a1, ServerAddress a2 )
        {
            return Integer.compare( a1.port(), a2.port() );
        }
    }
}
