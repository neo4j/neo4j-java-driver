/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.SessionMode;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ClusterDriverTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final Config config = Config.build().withLogging( new ConsoleLogging( Level.INFO ) ).toConfig();

    @Ignore
    public void shouldDiscoverServers() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "discover_servers.script" ), 9001 );
        URI uri = URI.create( "bolt+discovery://127.0.0.1:9001" );

        // When
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config ) )
        {
            // Then
            Set<BoltServerAddress> addresses = driver.servers();
            assertThat( addresses, hasSize( 3 ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9002 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9003 ) ) );
        }

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Ignore
    public void shouldDiscoverNewServers() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "discover_new_servers.script" ), 9001 );
        URI uri = URI.create( "bolt+discovery://127.0.0.1:9001" );

        // When
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config ) )
        {
            // Then
            Set<BoltServerAddress> addresses = driver.servers();
            assertThat( addresses, hasSize( 4 ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9002 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9003 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9004 ) ) );
        }

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Ignore
    public void shouldHandleEmptyResponse() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "handle_empty_response.script" ), 9001 );
        URI uri = URI.create( "bolt+discovery://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config ) )
        {
            Set<BoltServerAddress> servers = driver.servers();
            assertThat( servers, hasSize( 1 ) );
            assertThat( servers, hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ) );
        }

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Ignore
    public void shouldHandleAcquireReadSession() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a read server
        StubServer.start( resource( "read_server.script" ), 9005 );
        URI uri = URI.create( "bolt+discovery://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( SessionMode.READ ) )
        {
            List<String> result = session.run( "MATCH (n) RETURN n.name" ).list( new Function<Record,String>()
            {
                @Override
                public String apply( Record record )
                {
                    return record.get( "n.name" ).asString();
                }
            } );

            assertThat( result, equalTo( Arrays.asList( "Bob", "Alice", "Tina" ) ) );

        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Ignore
    public void shouldThrowSessionExpiredIfReadServerDisappears()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        //Expect
        exception.expect( SessionExpiredException.class );
        exception.expectMessage( "Server at 127.0.0.1:9005 is no longer available" );

        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a read server
        StubServer.start( resource( "dead_server.script" ), 9005 );
        URI uri = URI.create( "bolt+discovery://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( SessionMode.READ ) )
        {
            StatementResult run = session.run( "MATCH (n) RETURN n.name" );
            List<String> result = run.list( new Function<Record,String>()
            {
                @Override
                public String apply( Record record )
                {
                    return record.get( "n.name" ).asString();
                }
            } );

            assertThat( result, equalTo( Arrays.asList( "Bob", "Alice", "Tina" ) ) );

        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Ignore
    public void shouldThrowSessionExpiredIfWriteServerDisappears()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        //Expect
        exception.expect( SessionExpiredException.class );
        exception.expectMessage( "Server at 127.0.0.1:9006 is no longer available" );

        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a read server
        StubServer.start( resource( "dead_server.script" ), 9006 );
        URI uri = URI.create( "bolt+discovery://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( SessionMode.WRITE ) )
        {
            session.run( "MATCH (n) RETURN n.name" ).consume();
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Ignore
    public void shouldHandleAcquireWriteSession() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a write server
        StubServer.start( resource( "write_server.script" ), 9006 );
        URI uri = URI.create( "bolt+discovery://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( SessionMode.WRITE ) )
        {
            session.run( "CREATE (n {name:'Bob'})" );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    String resource( String fileName )
    {
        URL resource = ClusterDriverTest.class.getClassLoader().getResource( fileName );
        if ( resource == null )
        {
            fail( fileName + " does not exists" );
        }
        return resource.getFile();
    }
}