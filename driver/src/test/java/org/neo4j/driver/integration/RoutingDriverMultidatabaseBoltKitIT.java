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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.integration.RoutingDriverBoltKitIT.PortBasedServerAddressComparator;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.driver.util.StubServer;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.internal.InternalBookmark.parse;
import static org.neo4j.driver.util.StubServer.INSECURE_CONFIG;
import static org.neo4j.driver.util.StubServer.insecureBuilder;

class RoutingDriverMultidatabaseBoltKitIT
{
    @Test
    void shouldDiscoverForDatabase() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer router = StubServer.start( "acquire_endpoints_v4.script", 9001 );
        //START a read server
        StubServer reader = StubServer.start( "read_server_v4_read.script", 9005 );
        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).withDatabase( "mydatabase" ).build() ) )
        {
            List<String> result = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( "n.name" ).asString() );

            assertThat( result, equalTo( asList( "Bob", "Alice", "Tina" ) ) );
        }
        // Finally
        assertThat( router.exitStatus(), equalTo( 0 ) );
        assertThat( reader.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldRetryOnEmptyDiscoveryResult() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        ServerAddressResolver resolver = a -> {
            SortedSet<ServerAddress> addresses = new TreeSet<>( new PortBasedServerAddressComparator() );
            addresses.add( ServerAddress.of( "127.0.0.1", 9001 ) );
            addresses.add( ServerAddress.of( "127.0.0.1", 9002 ) );
            return addresses;
        };

        StubServer emptyRouter = StubServer.start( "acquire_endpoints_v4_empty.script", 9001 );
        StubServer realRouter = StubServer.start( "acquire_endpoints_v4_virtual_host.script", 9002 );
        StubServer reader = StubServer.start( "read_server_v4_read.script", 9005 );

        Config config = insecureBuilder().withResolver( resolver ).build();
        try ( Driver driver = GraphDatabase.driver( "neo4j://my.virtual.host:8080", config );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).withDatabase( "mydatabase" ).build() ) )
        {
            List<String> result = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( "n.name" ).asString() );

            assertThat( result, equalTo( asList( "Bob", "Alice", "Tina" ) ) );
        }
        // Finally
        assertThat( emptyRouter.exitStatus(), equalTo( 0 ) );
        assertThat( realRouter.exitStatus(), equalTo( 0 ) );
        assertThat( reader.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldThrowRoutingErrorIfDatabaseNotFound() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints_v4_database_not_found.script", 9001 );

        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );
                Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).withDatabase( "mydatabase" ).build() ) )
        {
            final FatalDiscoveryException error = assertThrows( FatalDiscoveryException.class, () -> {
                session.run( "MATCH (n) RETURN n.name" );
            } );

            assertThat( error.code(), equalTo( "Neo.ClientError.Database.DatabaseNotFound" ) );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    void shouldBeAbleToServeReachableDatabase() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer router = StubServer.start( "acquire_endpoints_v4_multi_db.script", 9001 );
        StubServer readServer = StubServer.start( "read_server_v4_read.script", 9005 );

        URI uri = URI.create( "neo4j://127.0.0.1:9001" );

        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            try( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).withDatabase( "unreachable" ).build() ) )
            {
                final ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, () -> {
                    session.run( "MATCH (n) RETURN n.name" );
                } );

                assertThat( error.getMessage(), containsString( "Could not perform discovery for database 'unreachable'" ) );
            }

            try ( Session session = driver.session( builder().withDefaultAccessMode( AccessMode.READ ).withDatabase( "mydatabase" ).build() ) )
            {
                List<String> result = session.run( "MATCH (n) RETURN n.name" ).list( record -> record.get( "n.name" ).asString() );

                assertThat( result, equalTo( asList( "Bob", "Alice", "Tina" ) ) );
            }
        }
        // Finally
        assertThat( router.exitStatus(), equalTo( 0 ) );
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
    }


    @Test
    void shouldDriverVerifyConnectivity() throws Throwable
    {
        StubServer router = StubServer.start( "acquire_endpoints_v4_verify_connectivity.script", 9001 );
        StubServer readServer = StubServer.start( "read_server_v4_read.script", 9005 );

        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            driver.verifyConnectivity();
            try ( Session session = driver.session( builder().withDatabase( "mydatabase" ).withDefaultAccessMode( AccessMode.READ ).build() ) )
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
    void shouldPassSystemBookmarkWhenGettingRoutingTableForMultiDB() throws Throwable
    {
        Bookmark sysBookmark = parse( "sys:1234" );
        Bookmark fooBookmark = parse( "foo:5678" );
        StubServer router = StubServer.start( "acquire_endpoints_v4_with_bookmark.script", 9001 );
        StubServer readServer = StubServer.start( "read_server_v4_read_with_bookmark.script", 9005 );

        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            try ( Session session = driver.session( builder()
                    .withDatabase( "foo" )
                    .withDefaultAccessMode( AccessMode.READ )
                    .withBookmarks( sysBookmark, fooBookmark )
                    .build() ) )
            {
                List<Record> records = session.run( "MATCH (n) RETURN n.name" ).list();
                assertEquals( 3, records.size() );
                assertThat( session.lastBookmark(), equalTo( parse( "foo:6678" ) ) );
            }
        }
        finally
        {
            assertEquals( 0, readServer.exitStatus() );
            assertEquals( 0, router.exitStatus() );
        }
    }

    @Test
    void shouldIgnoreSystemBookmarkWhenGettingRoutingTable() throws Throwable
    {
        Bookmark sysBookmark = parse( "sys:1234" );
        Bookmark fooBookmark = parse( "foo:5678" );
        StubServer router = StubServer.start( "acquire_endpoints_v3.script", 9001 );
        StubServer readServer = StubServer.start( "read_server_v3_read_with_bookmark.script", 9005 );

        URI uri = URI.create( "neo4j://127.0.0.1:9001" );
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            try ( Session session = driver.session( builder()
                    .withDefaultAccessMode( AccessMode.READ )
                    .withBookmarks( sysBookmark, fooBookmark ) // you can still send, the real server will reject in session run of course.
                    .build() ) )
            {
                List<Record> records = session.run( "MATCH (n) RETURN n.name" ).list();
                assertEquals( 3, records.size() );
                assertThat( session.lastBookmark(), equalTo( parse( "foo:6678" ) ) );
            }
        }
        finally
        {
            assertEquals( 0, readServer.exitStatus() );
            assertEquals( 0, router.exitStatus() );
        }
    }
}
