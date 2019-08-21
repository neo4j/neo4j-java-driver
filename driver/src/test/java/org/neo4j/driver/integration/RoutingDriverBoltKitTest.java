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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.Bookmark;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.DriverFactoryWithClock;
import org.neo4j.driver.internal.util.DriverFactoryWithFixedRetryLogic;
import org.neo4j.driver.internal.util.SleeplessClock;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.driver.util.StubServer;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

class RoutingDriverBoltKitTest
{
    @Test
    void shouldHandleAcquireReadSession() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a read server
        StubServer readServer = StubServer.start( "read_server_v3_read.script", 9005 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );

        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ); Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
        {
            List<String> result = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( "n.name" ).asString() );

            assertThat( result, equalTo( asList( "Bob", "Alice", "Tina" ) ) );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldHandleAcquireReadTransaction() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a read server
        StubServer readServer = StubServer.start( "read_server_v3_read_tx.script", 9005 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )

        {
            List<String> result = session.readTransaction( tx -> tx.run( "MATCH (n) RETURN n.name" ) )
                    .list( record -> record.get( "n.name" ).asString() );

            assertThat( result, equalTo( asList( "Bob", "Alice", "Tina" ) ) );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldHandleAcquireReadSessionAndTransaction() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a read server
        StubServer readServer = StubServer.start( "read_server_v3_read_tx.script", 9005 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() );
                Transaction tx = session.beginTransaction() )
        {
            List<String> result = tx.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( "n.name" ).asString() );

            assertThat( result, equalTo( asList( "Bob", "Alice", "Tina" ) ) );
            tx.success();
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldRoundRobinReadServers() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START two read servers
        StubServer readServer1 = StubServer.start( "read_server_v3_read.script", 9005 );
        StubServer readServer2 = StubServer.start( "read_server_v3_read.script", 9006 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            // Run twice, one on each read server
            for ( int i = 0; i < 2; i++ )
            {
                try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
                {
                    assertThat( session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( "n.name" ).asString() ),
                            equalTo( asList( "Bob", "Alice", "Tina" ) ) );
                }
            }
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer1.exitStatus(), equalTo( 0 ) );
        assertThat( readServer2.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldRoundRobinReadServersWhenUsingTransaction() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START two read servers
        StubServer readServer1 = StubServer.start( "read_server_v3_read_tx.script", 9005 );
        StubServer readServer2 = StubServer.start( "read_server_v3_read_tx.script", 9006 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            // Run twice, one on each read server
            for ( int i = 0; i < 2; i++ )
            {
                try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ); Transaction tx = session.beginTransaction() )
                {
                    assertThat( tx.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( "n.name" ).asString() ),
                            equalTo( asList( "Bob", "Alice", "Tina" ) ) );
                    tx.success();
                }
            }
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer1.exitStatus(), equalTo( 0 ) );
        assertThat( readServer2.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldThrowSessionExpiredIfReadServerDisappears() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a read server
        final StubServer readServer = StubServer.start( "dead_read_server.script", 9005 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );

        //Expect
        assertThrows( SessionExpiredException.class, () -> {
            try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                    Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
            {
                session.run( "MATCH (n) RETURN n.name" );
            }
        } );
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldThrowSessionExpiredIfReadServerDisappearsWhenUsingTransaction() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a read server
        final StubServer readServer = StubServer.start( "dead_read_server_tx.script", 9005 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );

        //Expect
        SessionExpiredException e = assertThrows( SessionExpiredException.class, () -> {
            try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                    Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() );
                    Transaction tx = session.beginTransaction() )
            {
                tx.run( "MATCH (n) RETURN n.name" );
                tx.success();
            }
        } );
        assertEquals( "Server at 127.0.0.1:9005 is no longer available", e.getMessage() );
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldThrowSessionExpiredIfWriteServerDisappears() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a dead write server
        final StubServer writeServer = StubServer.start( "dead_write_server.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );

        //Expect
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ) )
        {
            assertThrows( SessionExpiredException.class, () -> session.run( "CREATE (n {name:'Bob'})" ).consume() );
        }
        finally
        {
            assertThat( server.exitStatus(), equalTo( 0 ) );
            assertThat( writeServer.exitStatus(), equalTo( 0 ) );
        }
    }

    @Test
    void shouldThrowSessionExpiredIfWriteServerDisappearsWhenUsingTransaction() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a dead write servers
        final StubServer writeServer = StubServer.start( "dead_read_server_tx.script", 9007 );

        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        //Expect
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() );
                Transaction tx = session.beginTransaction() )
        {
            assertThrows( SessionExpiredException.class, () -> tx.run( "MATCH (n) RETURN n.name" ).consume() );
            tx.success();
        }
        finally
        {
            assertThat( server.exitStatus(), equalTo( 0 ) );
            assertThat( writeServer.exitStatus(), equalTo( 0 ) );
        }
    }

    @Test
    void shouldHandleAcquireWriteSession() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a write server
        StubServer writeServer = StubServer.start( "write_server_v3_write.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ) )
        {
            session.run( "CREATE (n {name:'Bob'})" );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldHandleAcquireWriteTransaction() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a write server
        StubServer writeServer = StubServer.start( "write_server_v3_write_tx.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ); Session session = driver.session() )
        {
            session.writeTransaction( t -> t.run( "CREATE (n {name:'Bob'})" ) );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldHandleAcquireWriteSessionAndTransaction() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a write server
        StubServer writeServer = StubServer.start( "write_server_v3_write_tx.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() );
                Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (n {name:'Bob'})" );
            tx.success();
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldRoundRobinWriteSessions() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a write server
        StubServer writeServer1 = StubServer.start( "write_server_v3_write.script", 9007 );
        StubServer writeServer2 = StubServer.start( "write_server_v3_write.script", 9008 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            for ( int i = 0; i < 2; i++ )
            {
                try ( Session session = driver.session() )
                {
                    session.run( "CREATE (n {name:'Bob'})" );
                }
            }
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer1.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer2.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldRoundRobinWriteSessionsInTransaction() throws Exception
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a write server
        StubServer writeServer1 = StubServer.start( "write_server_v3_write_tx.script", 9007 );
        StubServer writeServer2 = StubServer.start( "write_server_v3_write_tx.script", 9008 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            for ( int i = 0; i < 2; i++ )
            {
                try ( Session session = driver.session(); Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (n {name:'Bob'})" );
                    tx.success();
                }
            }
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer1.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer2.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldFailOnNonDiscoverableServer() throws IOException, InterruptedException
    {
        // Given
        StubServer.start( "discover_not_supported.script", 9001 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        final Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );

        //Expect
        assertThrows( ServiceUnavailableException.class, driver::verifyConnectivity );
    }

    @Test
    void shouldFailRandomFailureInGetServers() throws IOException, InterruptedException
    {
        // Given
        StubServer.start( "discover_failed.script", 9001 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        final Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );

        //Expect
        assertThrows( ServiceUnavailableException.class, driver::verifyConnectivity );
    }

    @Test
    void shouldHandleLeaderSwitchWhenWriting() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a write server that doesn't accept writes
        StubServer.start( "not_able_to_write_server.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
        boolean failed = false;
        try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ) )
        {
            session.run( "CREATE ()" ).consume();
        }
        catch ( SessionExpiredException e )
        {
            failed = true;
            assertThat( e.getMessage(), equalTo( "Server at 127.0.0.1:9007 no longer accepts writes" ) );
        }
        assertTrue( failed );

        driver.close();
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldHandleLeaderSwitchWhenWritingWithoutConsuming() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a write server that doesn't accept writes
        StubServer.start( "not_able_to_write_server.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
        boolean failed = false;
        try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ) )
        {
            session.run( "CREATE ()" );
        }
        catch ( SessionExpiredException e )
        {
            failed = true;
            assertThat( e.getMessage(), equalTo( "Server at 127.0.0.1:9007 no longer accepts writes" ) );
        }
        assertTrue( failed );

        driver.close();
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldHandleLeaderSwitchWhenWritingInTransaction() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v3.script", 9001 );

        //START a write server that doesn't accept writes
        StubServer.start( "not_able_to_write_server.script", 9007 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
        boolean failed = false;
        try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ); Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE ()" ).consume();
        }
        catch ( SessionExpiredException e )
        {
            failed = true;
            assertThat( e.getMessage(), equalTo( "Server at 127.0.0.1:9007 no longer accepts writes" ) );
        }
        assertTrue( failed );

        driver.close();
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldSendInitialBookmark() throws Exception
    {
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer writer = StubServer.start( "write_tx_with_bookmarks.script", 9007 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9001", INSECURE_CONFIG );
                Session session = driver.session( builder().withBookmarks( parse( "OldBookmark" ) ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (n {name:'Bob'})" );
                tx.success();
            }

            assertEquals( parse( "NewBookmark" ), session.lastBookmark() );
        }

        assertThat( router.exitStatus(), equalTo( 0 ) );
        assertThat( writer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldUseWriteSessionModeAndInitialBookmark() throws Exception
    {
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer writer = StubServer.start( "write_tx_with_bookmarks.script", 9008 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9001", INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).withBookmarks( parse( "OldBookmark" ) ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (n {name:'Bob'})" );
                tx.success();
            }

            assertEquals( parse( "NewBookmark" ), session.lastBookmark() );
        }

        assertThat( router.exitStatus(), equalTo( 0 ) );
        assertThat( writer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldUseReadSessionModeAndInitialBookmark() throws Exception
    {
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer writer = StubServer.start( "read_tx_with_bookmarks.script", 9005 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9001", INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).withBookmarks( parse( "OldBookmark" ) ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                List<Record> records = tx.run( "MATCH (n) RETURN n.name AS name" ).list();
                assertEquals( 2, records.size() );
                assertEquals( "Bob", records.get( 0 ).get( "name" ).asString() );
                assertEquals( "Alice", records.get( 1 ).get( "name" ).asString() );
                tx.success();
            }

            assertEquals( parse( "NewBookmark" ), session.lastBookmark() );
        }

        assertThat( router.exitStatus(), equalTo( 0 ) );
        assertThat( writer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldPassBookmarkFromTransactionToTransaction() throws Exception
    {
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer writer = StubServer.start( "write_read_tx_with_bookmarks.script", 9007 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9001", INSECURE_CONFIG );
                Session session = driver.session( builder().withBookmarks( parse( "BookmarkA" ) ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (n {name:'Bob'})" );
                tx.success();
            }

            assertEquals( parse( "BookmarkB" ), session.lastBookmark() );

            try ( Transaction tx = session.beginTransaction() )
            {
                List<Record> records = tx.run( "MATCH (n) RETURN n.name AS name" ).list();
                assertEquals( 1, records.size() );
                assertEquals( "Bob", records.get( 0 ).get( "name" ).asString() );
                tx.success();
            }

            assertEquals( parse( "BookmarkC" ), session.lastBookmark() );
        }

        assertThat( router.exitStatus(), equalTo( 0 ) );
        assertThat( writer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldRetryReadTransactionUntilSuccess() throws Exception
    {
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer brokenReader = StubServer.start( "dead_read_server_tx.script", 9005 );
        StubServer reader = StubServer.start( "read_server_v3_read_tx.script", 9006 );

        try ( Driver driver = newDriverWithSleeplessClock( "neo4j://127.0.0.1:9001" ); Session session = driver.session() )
        {
            AtomicInteger invocations = new AtomicInteger();
            List<Record> records = session.readTransaction( queryWork( "MATCH (n) RETURN n.name", invocations ) );

            assertEquals( 3, records.size() );
            assertEquals( 2, invocations.get() );
        }
        finally
        {
            assertEquals( 0, router.exitStatus() );
            assertEquals( 0, brokenReader.exitStatus() );
            assertEquals( 0, reader.exitStatus() );
        }
    }

    @Test
    void shouldRetryWriteTransactionUntilSuccess() throws Exception
    {
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer brokenWriter = StubServer.start( "dead_write_server.script", 9007 );
        StubServer writer = StubServer.start( "write_server_v3_write_tx.script", 9008 );

        try ( Driver driver = newDriverWithSleeplessClock( "neo4j://127.0.0.1:9001" ); Session session = driver.session() )
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
        StubServer router = StubServer.start( "acquire_endpoints_v3_leader_killed.script", 9001 );
        StubServer brokenWriter = StubServer.start( "dead_write_server.script", 9004 );
        StubServer writer = StubServer.start( "write_server_v3_write_tx.script", 9008 );

        Logger logger = mock( Logger.class );
        Config config = insecureBuilder().withLogging( ignored -> logger ).build();
        try ( Driver driver = newDriverWithSleeplessClock( "neo4j://127.0.0.1:9001", config );
                Session session = driver.session() )
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
        StubServer router = StubServer.start( "acquire_endpoints_v3_leader_killed.script", 9001 );
        StubServer brokenWriter = StubServer.start( "database_shutdown_at_commit.script", 9004 );
        StubServer writer = StubServer.start( "write_server_v3_write_tx.script", 9008 );

        Logger logger = mock( Logger.class );
        Config config = insecureBuilder().withLogging( ignored -> logger ).build();
        try ( Driver driver = newDriverWithSleeplessClock( "neo4j://127.0.0.1:9001", config );
                Session session = driver.session() )
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
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer brokenReader1 = StubServer.start( "dead_read_server_tx.script", 9005 );
        StubServer brokenReader2 = StubServer.start( "dead_read_server_tx.script", 9006 );

        try ( Driver driver = newDriverWithFixedRetries( "neo4j://127.0.0.1:9001", 1 );
              Session session = driver.session() )
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
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer brokenWriter1 = StubServer.start( "dead_write_server.script", 9007 );
        StubServer brokenWriter2 = StubServer.start( "dead_write_server.script", 9008 );

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
    void shouldRetryReadTransactionAndPerformRediscoveryUntilSuccess() throws Exception
    {
        StubServer router1 = StubServer.start( "acquire_endpoints_v3.script", 9010 );
        StubServer brokenReader1 = StubServer.start( "dead_read_server_tx.script", 9005 );
        StubServer brokenReader2 = StubServer.start( "dead_read_server_tx.script", 9006 );
        StubServer router2 = StubServer.start( "discover_servers.script", 9003 );
        StubServer reader = StubServer.start( "read_server_v3_read_tx.script", 9004 );

        try ( Driver driver = newDriverWithSleeplessClock( "neo4j://127.0.0.1:9010" ); Session session = driver.session() )
        {
            AtomicInteger invocations = new AtomicInteger();
            List<Record> records = session.readTransaction( queryWork( "MATCH (n) RETURN n.name", invocations ) );

            assertEquals( 3, records.size() );
            assertEquals( 3, invocations.get() );
        }
        finally
        {
            assertEquals( 0, router1.exitStatus() );
            assertEquals( 0, brokenReader1.exitStatus() );
            assertEquals( 0, brokenReader2.exitStatus() );
            assertEquals( 0, router2.exitStatus() );
            assertEquals( 0, reader.exitStatus() );
        }
    }

    @Test
    void shouldRetryWriteTransactionAndPerformRediscoveryUntilSuccess() throws Exception
    {
        StubServer router1 = StubServer.start( "discover_servers.script", 9010 );
        StubServer brokenWriter1 = StubServer.start( "dead_write_server.script", 9001 );
        StubServer router2 = StubServer.start( "acquire_endpoints_v3.script", 9002 );
        StubServer brokenWriter2 = StubServer.start( "dead_write_server.script", 9008 );
        StubServer writer = StubServer.start( "write_server_v3_write_tx.script", 9007 );

        try ( Driver driver = newDriverWithSleeplessClock( "neo4j://127.0.0.1:9010" ); Session session = driver.session() )
        {
            AtomicInteger invocations = new AtomicInteger();
            List<Record> records = session.writeTransaction( queryWork( "CREATE (n {name:'Bob'})", invocations ) );

            assertEquals( 0, records.size() );
            assertEquals( 3, invocations.get() );
        }
        finally
        {
            assertEquals( 0, router1.exitStatus() );
            assertEquals( 0, brokenWriter1.exitStatus() );
            assertEquals( 0, router2.exitStatus() );
            assertEquals( 0, writer.exitStatus() );
            assertEquals( 0, brokenWriter2.exitStatus() );
        }
    }

    @Test
    void shouldUseInitialRouterForRediscoveryWhenAllOtherRoutersAreDead() throws Exception
    {
        // initial router does not have itself in the returned set of routers
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9010 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9010", INSECURE_CONFIG ) )
        {
            driver.verifyConnectivity();
            try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
            {
                // restart router on the same port with different script that contains itself as reader
                assertEquals( 0, router.exitStatus() );
                router = StubServer.start( "rediscover_using_initial_router.script", 9010 );

                List<String> names = readStrings( "MATCH (n) RETURN n.name AS name", session );
                assertEquals( asList( "Bob", "Alice" ), names );
            }
        }

        assertEquals( 0, router.exitStatus() );
    }

    @Test
    void shouldInvokeProcedureGetRoutingTableWhenServerVersionPermits() throws Exception
    {
        // stub server is both a router and reader
        StubServer server = StubServer.start( "get_routing_table.script", 9001 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9001", INSECURE_CONFIG ); Session session = driver.session() )
        {
            List<Record> records = session.run( "MATCH (n) RETURN n.name AS name" ).list();
            assertEquals( 3, records.size() );
            assertEquals( "Alice", records.get( 0 ).get( "name" ).asString() );
            assertEquals( "Bob", records.get( 1 ).get( "name" ).asString() );
            assertEquals( "Eve", records.get( 2 ).get( "name" ).asString() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    @Test
    void shouldSendRoutingContextToServer() throws Exception
    {
        // stub server is both a router and reader
        StubServer server = StubServer.start( "get_routing_table_with_context.script", 9001 );

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
    void shouldServeReadsButFailWritesWhenNoWritersAvailable() throws Exception
    {
        StubServer router1 = StubServer.start( "discover_no_writers.script", 9010 );
        StubServer router2 = StubServer.start( "discover_no_writers.script", 9004 );
        StubServer reader = StubServer.start( "read_server_v3_read_tx.script", 9003 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9010", INSECURE_CONFIG );
                Session session = driver.session() )
        {
            assertEquals( asList( "Bob", "Alice", "Tina" ), readStrings( "MATCH (n) RETURN n.name", session ) );

            assertThrows( SessionExpiredException.class, () -> session.run( "CREATE (n {name:'Bob'})" ).consume() );
        }
        finally
        {
            assertEquals( 0, router1.exitStatus() );
            assertEquals( 0, router2.exitStatus() );
            assertEquals( 0, reader.exitStatus() );
        }
    }

    @Test
    void shouldAcceptRoutingTableWithoutWritersAndThenRediscover() throws Exception
    {
        // first router does not have itself in the resulting routing table so connection
        // towards it will be closed after rediscovery
        StubServer router1 = StubServer.start( "discover_no_writers.script", 9010 );
        StubServer router2 = null;
        StubServer reader = StubServer.start( "read_server_v3_read_tx.script", 9003 );
        StubServer writer = StubServer.start( "write_with_bookmarks.script", 9007 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9010", INSECURE_CONFIG ) )
        {
            driver.verifyConnectivity();
            try ( Session session = driver.session() )
            {
                // start another router which knows about writes, use same address as the initial router
                router2 = StubServer.start( "acquire_endpoints_v3.script", 9010 );

                assertEquals( asList( "Bob", "Alice", "Tina" ), readStrings( "MATCH (n) RETURN n.name", session ) );

                StatementResult createResult = session.run( "CREATE (n {name:'Bob'})" );
                assertFalse( createResult.hasNext() );
            }
        }
        finally
        {
            assertEquals( 0, router1.exitStatus() );
            assertNotNull( router2 );
            assertEquals( 0, router2.exitStatus() );
            assertEquals( 0, reader.exitStatus() );
            assertEquals( 0, writer.exitStatus() );
        }
    }

    @Test
    void shouldTreatRoutingTableWithSingleRouterAsValid() throws Exception
    {
        StubServer router = StubServer.start( "discover_one_router.script", 9010 );
        StubServer reader1 = StubServer.start( "read_server_v3_read.script", 9003 );
        StubServer reader2 = StubServer.start( "read_server_v3_read.script", 9004 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9010", INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
        {
            // returned routing table contains only one router, this should be fine and we should be able to
            // read multiple times without additional rediscovery

            StatementResult readResult1 = session.run( "MATCH (n) RETURN n.name" );
            assertEquals( "127.0.0.1:9003", readResult1.summary().server().address() );
            assertEquals( 3, readResult1.list().size() );

            StatementResult readResult2 = session.run( "MATCH (n) RETURN n.name" );
            assertEquals( "127.0.0.1:9004", readResult2.summary().server().address() );
            assertEquals( 3, readResult2.list().size() );
        }
        finally
        {
            assertEquals( 0, router.exitStatus() );
            assertEquals( 0, reader1.exitStatus() );
            assertEquals( 0, reader2.exitStatus() );
        }
    }

    @Test
    void shouldSendMultipleBookmarks() throws Exception
    {
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer writer = StubServer.start( "multiple_bookmarks.script", 9007 );

        Bookmark bookmark = parse(
                asList( "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29", "neo4j:bookmark:v1:tx94", "neo4j:bookmark:v1:tx56", "neo4j:bookmark:v1:tx16",
                        "neo4j:bookmark:v1:tx68" ) );

        try ( Driver driver = GraphDatabase.driver( "neo4j://localhost:9001", INSECURE_CONFIG ); Session session = driver.session( builder().withBookmarks( bookmark ).build() ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (n {name:'Bob'})" );
                tx.success();
            }

            assertEquals( parse( "neo4j:bookmark:v1:tx95" ), session.lastBookmark() );
        }
        finally
        {
            assertEquals( 0, router.exitStatus() );
            assertEquals( 0, writer.exitStatus() );
        }
    }

    @Test
    void shouldForgetAddressOnDatabaseUnavailableError() throws Exception
    {
        // perform initial discovery using router1
        StubServer router1 = StubServer.start( "discover_servers.script", 9010 );
        // attempt to write using writer1 which fails with 'Neo.TransientError.General.DatabaseUnavailable'
        // it should then be forgotten and trigger new rediscovery
        StubServer writer1 = StubServer.start( "writer_unavailable.script", 9001 );
        // perform rediscovery using router2, it should return a valid writer2
        StubServer router2 = StubServer.start( "acquire_endpoints_v3.script", 9002 );
        // write on writer2 should be successful
        StubServer writer2 = StubServer.start( "write_server_v3_write_tx.script", 9007 );

        try ( Driver driver = newDriverWithSleeplessClock( "neo4j://localhost:9010" ); Session session = driver.session() )
        {
            AtomicInteger invocations = new AtomicInteger();
            List<Record> records = session.writeTransaction( queryWork( "CREATE (n {name:'Bob'})", invocations ) );

            assertThat( records, hasSize( 0 ) );
            assertEquals( 2, invocations.get() );
        }
        finally
        {
            assertEquals( 0, router1.exitStatus() );
            assertEquals( 0, writer1.exitStatus() );
            assertEquals( 0, router2.exitStatus() );
            assertEquals( 0, writer2.exitStatus() );
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
    void shouldUseResolverDuringRediscoveryWhenExistingRoutersFail() throws Exception
    {
        StubServer router1 = StubServer.start( "get_routing_table.script", 9001 );
        StubServer router2 = StubServer.start( "acquire_endpoints_v3.script", 9042 );
        StubServer reader = StubServer.start( "read_server_v3_read_tx.script", 9005 );

        AtomicBoolean resolverInvoked = new AtomicBoolean();
        ServerAddressResolver resolver = address -> {
            if ( resolverInvoked.compareAndSet( false, true ) )
            {
                // return the address first time
                return singleton( address );
            }
            if ( "127.0.0.1".equals( address.host() ) && address.port() == 9001 )
            {
                // return list of addresses where onl 9042 is functional
                return new HashSet<>(
                        asList( ServerAddress.of( "127.0.0.1", 9010 ), ServerAddress.of( "127.0.0.1", 9011 ), ServerAddress.of( "127.0.0.1", 9042 ) ) );
            }
            throw new AssertionError();
        };

        Config config = insecureBuilder().withResolver( resolver ).build();

        try ( Driver driver = GraphDatabase.driver( "neo4j://127.0.0.1:9001", config ) )
        {
            try ( Session session = driver.session() )
            {
                // run first query against 9001, which should return result and exit
                List<String> names1 = session.run( "MATCH (n) RETURN n.name AS name" ).list( record -> record.get( "name" ).asString() );
                assertEquals( asList( "Alice", "Bob", "Eve" ), names1 );

                // run second query with retries, it should rediscover using 9042 returned by the resolver and read from 9005
                List<String> names2 = session.readTransaction( tx -> tx.run( "MATCH (n) RETURN n.name" ) ).list( record -> record.get( 0 ).asString() );
                assertEquals( asList( "Bob", "Alice", "Tina" ), names2 );
            }
        }
        finally
        {
            assertEquals( 0, router1.exitStatus() );
            assertEquals( 0, router2.exitStatus() );
            assertEquals( 0, reader.exitStatus() );
        }
    }

    @Test
    void useSessionAfterDriverIsClosed() throws Exception
    {
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer readServer = StubServer.start( "read_server_v3_read.script", 9005 );

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
    void shouldRevertToInitialRouterIfKnownRouterThrowsProtocolErrors() throws Exception
    {
        ServerAddressResolver resolver = a -> {
            SortedSet<ServerAddress> addresses = new TreeSet<>( new PortBasedServerAddressComparator() );
            addresses.add( ServerAddress.of( "127.0.0.1", 9001 ) );
            addresses.add( ServerAddress.of( "127.0.0.1", 9003 ) );
            return addresses;
        };

        Config config = insecureBuilder().withResolver( resolver ).build();

        StubServer router1 = StubServer.start( "acquire_endpoints_v3_point_to_empty_router_and_exit.script", 9001 );
        StubServer router2 = StubServer.start( "acquire_endpoints_v3_empty.script", 9004 );
        StubServer router3 = StubServer.start( "acquire_endpoints_v3_three_servers_and_exit.script", 9003 );
        StubServer reader = StubServer.start( "read_server_v3_read_tx.script", 9002 );

        try ( Driver driver = GraphDatabase.driver( "neo4j://my.virtual.host:8080", config ) )
        {
            try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).build() ) )
            {
                List<Record> records = session.readTransaction( tx -> tx.run( "MATCH (n) RETURN n.name" ) ).list();
                assertEquals( 3, records.size() );
            }
        }
        finally
        {
            assertEquals( 0, router1.exitStatus() );
            assertEquals( 0, router2.exitStatus() );
            assertEquals( 0, router3.exitStatus() );
            assertEquals( 0, reader.exitStatus() );
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
        return driverFactory.newInstance( uri, auth, routingConf, RetrySettings.DEFAULT, config );
    }

    private static TransactionWork<List<Record>> queryWork( final String query, final AtomicInteger invocations )
    {
        return tx -> {
            invocations.incrementAndGet();
            return tx.run( query ).list();
        };
    }

    private static List<String> readStrings( final String query, Session session )
    {
        return session.readTransaction( tx -> {
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
