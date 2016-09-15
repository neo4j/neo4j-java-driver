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
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterDriverTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final Config config = Config.build().withLogging( new ConsoleLogging( Level.INFO ) ).toConfig();

    @Test
    public void shouldDiscoverServers() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "discover_servers.script" ), 9001 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        // When
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config ) )
        {
            // Then
            Set<BoltServerAddress> addresses = driver.discoveryServers();
            assertThat( addresses, hasSize( 3 ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9002 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9003 ) ) );
        }

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldDiscoverNewServers() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "discover_new_servers.script" ), 9001 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        // When
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config ) )
        {
            // Then
            Set<BoltServerAddress> addresses = driver.discoveryServers();
            assertThat( addresses, hasSize( 4 ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9002 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9003 ) ) );
            assertThat( addresses, hasItem( new BoltServerAddress( "127.0.0.1", 9004 ) ) );
        }

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldHandleEmptyResponse() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "handle_empty_response.script" ), 9001 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config ) )
        {
            Set<BoltServerAddress> servers = driver.discoveryServers();
            assertThat( servers, hasSize( 1 ) );
            assertThat( servers, hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ) );
        }

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldHandleAcquireReadSession() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a read server
        StubServer.start( resource( "read_server.script" ), 9005 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.READ ) )
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

    @Test
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
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.READ ) )
        {
            session.run( "MATCH (n) RETURN n.name" );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
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
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.WRITE ) )
        {
            session.run( "MATCH (n) RETURN n.name" ).consume();
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldHandleAcquireWriteSession() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a write server
        StubServer.start( resource( "write_server.script" ), 9006 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.WRITE ) )
        {
            session.run( "CREATE (n {name:'Bob'})" );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldRememberEndpoints() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a read server
        StubServer.start( resource( "read_server.script" ), 9005 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.READ ) )
        {
            session.run( "MATCH (n) RETURN n.name" ).consume();

            assertThat( driver.readServers(), hasSize( 1 ));
            assertThat( driver.readServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9005 ) ) );
            assertThat( driver.writeServers(), hasSize( 1 ));
            assertThat( driver.writeServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9006 ) ) );
            //Make sure we don't cache acquired servers as discovery servers
            assertThat( driver.discoveryServers(), not(hasItem(  new BoltServerAddress( "127.0.0.1", 9005 ))));
            assertThat( driver.discoveryServers(), not(hasItem(  new BoltServerAddress( "127.0.0.1", 9006 ))));
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldForgetEndpointsOnFailure() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a read server
        StubServer.start( resource( "dead_server.script" ), 9005 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
        boolean failed = false;
        try
        {
            Session session = driver.session( AccessMode.READ );
            session.run( "MATCH (n) RETURN n.name" ).consume();
            session.close();
        }
        catch ( SessionExpiredException e )
        {
            failed = true;
        }

        assertTrue( failed );
        assertThat( driver.readServers(), hasSize( 0 ) );
        assertThat( driver.writeServers(), hasSize( 1 ) );
        driver.close();

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldRediscoverIfNecessaryOnSessionAcquisition() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "rediscover.script" ), 9001 );

        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        //START a read server
        StubServer.start( resource( "read_server.script" ), 9005 );

        //On creation we only find ourselves
        ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
        assertThat( driver.discoveryServers(), hasSize( 1 ) );
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ));

        //since we know about less than three servers a rediscover should be triggered
        Session session = driver.session( AccessMode.READ );
        assertThat( driver.discoveryServers(), hasSize( 4 ) );
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ));
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9002 ) ));
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9003 ) ));
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9004 ) ));

        session.close();
        driver.close();

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Ignore
    public void shouldOnlyDiscoverOnce() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "rediscover.script" ), 9001 );

        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        //START a read server
        StubServer.start( resource( "read_server.script" ), 9005 );

        //On creation we only find ourselves
        final ClusterDriver driver = (ClusterDriver) GraphDatabase.driver( uri, config );
        assertThat( driver.discoveryServers(), hasSize( 1 ) );
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ));

        ExecutorService runner = Executors.newFixedThreadPool( 10 );
        for ( int i = 0; i < 10; i++ )
        {
            runner.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    //noinspection EmptyTryBlock
                    try(Session ignore = driver.session( AccessMode.READ ))
                    {
                        //empty
                    }

                }
            } );
        }
        runner.awaitTermination( 10, TimeUnit.SECONDS );
        //since we know about less than three servers a rediscover should be triggered
        assertThat( driver.discoveryServers(), hasSize( 4 ) );
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9001 ) ));
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9002 ) ));
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9003 ) ));
        assertThat( driver.discoveryServers(), hasItem( new BoltServerAddress( "127.0.0.1", 9004 ) ));

        driver.close();

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldFailOnNonDiscoverableServer() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // When
        StubServer server = StubServer.start( resource( "non_discovery_server.script" ), 9001 );

        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        boolean failed = false;
        //noinspection EmptyTryBlock
        try
        {
            Driver ignore = GraphDatabase.driver( uri, config );
        }
        catch ( ServiceUnavailableException e )
        {
            failed = true;
        }
        assertTrue( failed );

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