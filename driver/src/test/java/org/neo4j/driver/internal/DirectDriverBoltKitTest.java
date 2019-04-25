/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.ChannelTrackingDriverFactory;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.util.StubServer;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.StubServer.INSECURE_CONFIG;

class DirectDriverBoltKitTest
{
    @Test
    void shouldBeAbleRunCypher() throws Exception
    {
        StubServer server = StubServer.start( "return_x.script", 9001 );
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
        StubServer server = StubServer.start( "multiple_bookmarks.script", 9001 );

        List<String> bookmarks = asList( "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29",
                "neo4j:bookmark:v1:tx94", "neo4j:bookmark:v1:tx56", "neo4j:bookmark:v1:tx16",
                "neo4j:bookmark:v1:tx68" );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
              Session session = driver.session( bookmarks ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (n {name:'Bob'})" );
                tx.success();
            }

            assertEquals( "neo4j:bookmark:v1:tx95", session.lastBookmark() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldLogConnectionIdInDebugMode() throws Exception
    {
        StubServer server = StubServer.start( "hello_run_exit.script", 9001 );

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
    void shouldSendReadAccessModeInStatementMetadata() throws Exception
    {
        StubServer server = StubServer.start( "hello_run_exit_read.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session( AccessMode.READ ) )
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
    void shouldNotSendWriteAccessModeInStatementMetadata() throws Exception
    {
        StubServer server = StubServer.start( "hello_run_exit.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session( AccessMode.WRITE ) )
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
        StubServer server = StubServer.start( "reset_error.script", 9001 );
        try
        {
            URI uri = URI.create( "bolt://localhost:9001" );
            Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).withoutEncryption().build();
            ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );

            try ( Driver driver = driverFactory.newInstance( uri, AuthTokens.none(), RoutingSettings.DEFAULT, RetrySettings.DEFAULT, config ) )
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
    void shouldPropagateTransactionCommitErrorWhenSessionClosed() throws Exception
    {
        testTransactionCloseErrorPropagationWhenSessionClosed( "commit_error.script", true, "Unable to commit" );
    }

    @Test
    void shouldPropagateTransactionRollbackErrorWhenSessionClosed() throws Exception
    {
        testTransactionCloseErrorPropagationWhenSessionClosed( "rollback_error.script", false, "Unable to rollback" );
    }

    @Test
    void shouldThrowCommitErrorWhenTransactionClosed() throws Exception
    {
        testTxCloseErrorPropagation( "commit_error.script", true, "Unable to commit" );
    }

    @Test
    void shouldThrowRollbackErrorWhenTransactionClosed() throws Exception
    {
        testTxCloseErrorPropagation( "rollback_error.script", false, "Unable to rollback" );
    }

    @Test
    void shouldThrowCorrectErrorOnRunFailure() throws Throwable
    {
        StubServer server = StubServer.start( "database_shutdown.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session( "neo4j:bookmark:v1:tx0" );
                // has to enforce to flush BEGIN to have tx started.
                Transaction transaction = session.beginTransaction() )
        {
            TransientException error = assertThrows( TransientException.class, () -> {
                StatementResult result = transaction.run( "RETURN 1" );
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
        StubServer server = StubServer.start( "database_shutdown_at_commit.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                Session session = driver.session() )
        {
            Transaction transaction = session.beginTransaction();
            StatementResult result = transaction.run( "CREATE (n {name:'Bob'})" );
            result.consume();
            transaction.success();

            TransientException error = assertThrows( TransientException.class, transaction::close );
            assertThat( error.code(), equalTo( "Neo.TransientError.General.DatabaseUnavailable" ) );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    private static void testTransactionCloseErrorPropagationWhenSessionClosed( String script, boolean commit,
            String expectedErrorMessage ) throws Exception
    {
        StubServer server = StubServer.start( script, 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG ) )
            {
                Session session = driver.session();

                Transaction tx = session.beginTransaction();
                StatementResult result = tx.run( "CREATE (n {name:'Alice'}) RETURN n.name AS name" );
                assertEquals( "Alice", result.single().get( "name" ).asString() );

                if ( commit )
                {
                    tx.success();
                }
                else
                {
                    tx.failure();
                }

                TransientException e = assertThrows( TransientException.class, session::close );
                assertEquals( "Neo.TransientError.General.DatabaseUnavailable", e.code() );
                assertEquals( expectedErrorMessage, e.getMessage() );
            }
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    private static void testTxCloseErrorPropagation( String script, boolean commit, String expectedErrorMessage )
            throws Exception
    {
        StubServer server = StubServer.start( script, 9001 );
        try
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
                  Session session = driver.session() )
            {
                Transaction tx = session.beginTransaction();
                StatementResult result = tx.run( "CREATE (n {name:'Alice'}) RETURN n.name AS name" );
                assertEquals( "Alice", result.single().get( "name" ).asString() );

                if ( commit )
                {
                    tx.success();
                }
                else
                {
                    tx.failure();
                }

                TransientException e = assertThrows( TransientException.class, tx::close );
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
