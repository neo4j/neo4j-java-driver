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
package org.neo4j.driver.internal;

import io.netty.channel.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactory;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.util.StubServer;
import org.neo4j.driver.util.StubServerController;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.util.StubServer.INSECURE_CONFIG;
import static org.neo4j.driver.util.StubServer.insecureBuilder;
import static org.neo4j.driver.util.StubServer.start;
import static org.neo4j.driver.util.TestUtil.asOrderedSet;
import static org.neo4j.driver.util.TestUtil.await;

class DirectDriverBoltKitIT
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
    void shouldBeAbleRunCypher() throws Exception
    {
        StubServer server = stubController.startStub( "return_x.script", 9001 );
        URI uri = URI.create( "bolt://127.0.0.1:9001" );
        int x;

        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            try ( Session session = driver.session() )
            {
                Record record = session.run( "RETURN {x}", parameters( "x", 1 ) ).single();
                x = record.get( 0 ).asInt();
            }
        }

        assertThat( x, equalTo( 1 ) );
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldSendMultipleBookmarks() throws Exception
    {
        StubServer server = stubController.startStub( "multiple_bookmarks.script", 9001 );

        Bookmark bookmarks = InternalBookmark.parse( asOrderedSet( "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29",
                "neo4j:bookmark:v1:tx94", "neo4j:bookmark:v1:tx56", "neo4j:bookmark:v1:tx16", "neo4j:bookmark:v1:tx68" ) );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
              Session session = driver.session( builder().withBookmarks( bookmarks ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (n {name:'Bob'})" );
                tx.commit();
            }

            assertEquals( InternalBookmark.parse( "neo4j:bookmark:v1:tx95" ), session.lastBookmark() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldSendNullRoutingContextForBoltUri() throws Exception
    {
        StubServer server = StubServer.start( "hello_with_routing_context_bolt.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
              Session session = driver.session() )
        {
            List<String> names = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( 0 ).asString() );
            assertEquals( asList( "Foo", "Bar" ), names );

        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldLogConnectionIdInDebugMode() throws Exception
    {
        StubServer server = stubController.startStub( "hello_run_exit.script", 9001 );

        Logger logger = mock( Logger.class );
        when( logger.isDebugEnabled() ).thenReturn( true );

        Config config = Config.builder()
                .withLogging( ignore -> logger )
                .withoutEncryption()
                .build();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", config );
              Session session = driver.session() )
        {
            List<String> names = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( 0 ).asString() );
            assertEquals( asList( "Foo", "Bar" ), names );

            ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass( String.class );
            verify( logger, atLeastOnce() ).debug( messageCaptor.capture(), any() );

            Optional<String> logMessageWithConnectionId = messageCaptor.getAllValues()
                    .stream()
                    .filter( line -> line.contains( "bolt-123456789" ) )
                    .findAny();

            assertTrue( logMessageWithConnectionId.isPresent(),
                    "Expected log call did not happen. All debug log calls:\n" + String.join( "\n", messageCaptor.getAllValues() ) );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldSendReadAccessModeInQueryMetadata() throws Exception
    {
        StubServer server = stubController.startStub( "hello_run_exit_read.script", 9001 );


        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
        {
            List<String> names = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( 0 ).asString() );
            assertEquals( asList( "Foo", "Bar" ), names );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldNotSendWriteAccessModeInQueryMetadata() throws Exception
    {
        StubServer server = stubController.startStub( "hello_run_exit.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ) )
        {
            List<String> names = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( 0 ).asString() );
            assertEquals( asList( "Foo", "Bar" ), names );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldCloseChannelWhenResetFails() throws Exception
    {
        StubServer server = stubController.startStub( "reset_error.script", 9001 );
        try
        {
            URI uri = URI.create( "bolt://localhost:9001" );
            Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).withoutEncryption().build();
            ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );

            try ( Driver driver = driverFactory.newInstance( uri, AuthTokens.none(), RoutingSettings.DEFAULT, RetrySettings.DEFAULT, config,
                                                             SecurityPlanImpl.insecure() ) )
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

    @Test
    void shouldPropagateTransactionRollbackErrorWhenSessionClosed() throws Exception
    {
        StubServer server = stubController.startStub( "rollback_error.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                Session session = driver.session();

                Transaction tx = session.beginTransaction();
                Result result = tx.run( "CREATE (n {name:'Alice'}) RETURN n.name AS name" );
                assertEquals( "Alice", result.single().get( "name" ).asString() );

                TransientException e = assertThrows( TransientException.class, session::close );
                assertEquals( "Neo.TransientError.General.DatabaseUnavailable", e.code() );
                assertEquals( "Unable to rollback", e.getMessage() );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldStreamingRecordsInBatchesRx() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4_rx.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                RxSession session = driver.rxSession();
                RxResult result = session.run( "MATCH (n) RETURN n.name" );
                Flux<String> records = Flux.from( result.records() ).limitRate( 2 ).map( record -> record.get( "n.name" ).asString() );
                StepVerifier.create( records ).expectNext( "Bob", "Alice", "Tina" ).verifyComplete();
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldStreamingRecordsInBatches() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", insecureBuilder().withFetchSize( 2 ).build() ) )
            {
                Session session = driver.session();
                Result result = session.run( "MATCH (n) RETURN n.name" );
                List<String> list = result.list( record -> record.get( "n.name" ).asString() );
                assertEquals( list, asList( "Bob", "Alice", "Tina" ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldChangeFetchSize() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                Session session = driver.session( builder().withFetchSize( 2 ).build() );
                Result result = session.run( "MATCH (n) RETURN n.name" );
                List<String> list = result.list( record -> record.get( "n.name" ).asString() );
                assertEquals( list, asList( "Bob", "Alice", "Tina" ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldOnlyPullRecordsWhenNeededSimpleSession() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4_buffering.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                Session session = driver.session( builder().withFetchSize( 2 ).build() );
                Result result = session.run( "MATCH (n) RETURN n.name" );
                ArrayList<String> resultList = new ArrayList<>();
                result.forEachRemaining( ( rec ) -> resultList.add( rec.get( 0 ).asString() ) );

                assertEquals( resultList, asList( "Bob", "Alice", "Tina", "Frank", "Daisy", "Clive" ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldOnlyPullRecordsWhenNeededAsyncSession() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4_buffering.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                AsyncSession session = driver.asyncSession( builder().withFetchSize( 2 ).build() );

                ArrayList<String> resultList = new ArrayList<>();

                await( session.runAsync( "MATCH (n) RETURN n.name" )
                              .thenCompose( resultCursor ->
                                                    resultCursor.forEachAsync( record -> resultList.add( record.get( 0 ).asString() ) ) ) );

                assertEquals( resultList, asList( "Bob", "Alice", "Tina", "Frank", "Daisy", "Clive" ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldPullAllRecordsOnListAsyncWhenOverWatermark() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4_list_async.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                AsyncSession session = driver.asyncSession( builder().withFetchSize( 10 ).build() );

                ResultCursor cursor = await( session.runAsync( "MATCH (n) RETURN n.name" ) );
                List<String> records = await( cursor.listAsync( record -> record.get( 0 ).asString() ) );

                assertEquals( records, asList( "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L" ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldAllowPullAll() throws Exception
    {
        StubServer server = stubController.startStub( "streaming_records_v4_all.script", 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", insecureBuilder().withFetchSize( -1 ).build() ) )
            {
                Session session = driver.session();
                Result result = session.run( "MATCH (n) RETURN n.name" );
                List<String> list = result.list( record -> record.get( "n.name" ).asString() );
                assertEquals( list, asList( "Bob", "Alice", "Tina" ) );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldThrowCommitErrorWhenTransactionCommit() throws Exception
    {
        testTxCloseErrorPropagation( "commit_error.script", Transaction::commit, "Unable to commit" );
    }

    @Test
    void shouldThrowRollbackErrorWhenTransactionRollback() throws Exception
    {
        testTxCloseErrorPropagation( "rollback_error.script", Transaction::rollback, "Unable to rollback" );
    }

    @Test
    void shouldThrowRollbackErrorWhenTransactionClose() throws Exception
    {
        testTxCloseErrorPropagation( "rollback_error.script", Transaction::close, "Unable to rollback" );
    }


    @Test
    void shouldThrowCorrectErrorOnRunFailure() throws Throwable
    {
        StubServer server = stubController.startStub( "database_shutdown.script", 9001 );

        Bookmark bookmark = InternalBookmark.parse( "neo4j:bookmark:v1:tx0" );
        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session( builder().withBookmarks( bookmark ).build() );
                // has to enforce to flush BEGIN to have tx started.
                Transaction transaction = session.beginTransaction() )
        {
            TransientException error = assertThrows( TransientException.class, () -> {
                Result result = transaction.run( "RETURN 1" );
                result.consume();
            } );
            assertThat( error.code(), equalTo( "Neo.TransientError.General.DatabaseUnavailable" ) );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldThrowCorrectErrorOnCommitFailure() throws Throwable
    {
        StubServer server = stubController.startStub( "database_shutdown_at_commit.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session() )
        {
            Transaction transaction = session.beginTransaction();
            Result result = transaction.run( "CREATE (n {name:'Bob'})" );
            result.consume();

            TransientException error = assertThrows( TransientException.class, transaction::commit );
            assertThat( error.code(), equalTo( "Neo.TransientError.General.DatabaseUnavailable" ) );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldAllowDatabaseNameInSessionRun() throws Throwable
    {
        StubServer server = stubController.startStub( "read_server_v4_read.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session( builder().withDatabase( "mydatabase" ).withDefaultAccessMode( AccessMode.READ ).build() ) )
        {
            final Result result = session.run( "MATCH (n) RETURN n.name" );
            result.consume();
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldAllowDatabaseNameInBeginTransaction() throws Throwable
    {
        StubServer server = stubController.startStub( "read_server_v4_read_tx.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session( forDatabase( "mydatabase" ) ) )
        {
            session.readTransaction( tx -> tx.run( "MATCH (n) RETURN n.name" ).consume() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldDiscardIfPullNotFinished() throws Throwable
    {
        StubServer server = stubController.startStub( "read_tx_v4_discard.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
        {
            Flux<List<String>> keys = Flux.usingWhen(
                    Mono.fromSupplier( driver::rxSession ),
                    session -> session.readTransaction( tx -> tx.run( "UNWIND [1,2,3,4] AS a RETURN a" ).keys() ),
                    RxSession::close );
            StepVerifier.create( keys ).expectNext( singletonList( "a" ) ).verifyComplete();
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldServerWithBoltV4SupportMultiDb() throws Throwable
    {
        StubServer server = stubController.startStub( "support_multidb_v4.script", 9001 );
        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
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
        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
        {
            assertFalse( driver.supportsMultiDb() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldBeAbleHandleNOOPsDuringRunCypher() throws Exception
    {
        StubServer server = stubController.startStub( "noop.script", 9001 );
        URI uri = URI.create( "bolt://127.0.0.1:9001" );

        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            try ( Session session = driver.session() )
            {
                List<String> names = session.run( "MATCH (n) RETURN n.name" ).list( rec -> rec.get( 0 ).asString() );
                assertEquals( asList( "Foo", "Bar", "Baz" ), names );
            }
        }

        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldSendCustomerUserAgentInHelloMessage() throws Exception
    {
        StubServer server = stubController.startStub( "hello_with_custom_user_agent.script", 9001 );

        Config config = Config.builder().withUserAgent( "AwesomeClient" ).build();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", config );
              Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ) )
        {
            List<String> names = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( 0 ).asString() );
            assertEquals( asList( "Foo", "Bar" ), names );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    private static void testTxCloseErrorPropagation( String script, Consumer<Transaction> txAction, String expectedErrorMessage )
            throws Exception
    {
        StubServer server = stubController.startStub( script, 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                  Session session = driver.session() )
            {
                Transaction tx = session.beginTransaction();
                Result result = tx.run( "CREATE (n {name:'Alice'}) RETURN n.name AS name" );
                assertEquals( "Alice", result.single().get( "name" ).asString() );

                TransientException e = assertThrows( TransientException.class, () -> txAction.accept( tx ) );

                assertEquals( "Neo.TransientError.General.DatabaseUnavailable", e.code() );
                assertEquals( expectedErrorMessage, e.getMessage() );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }
}
