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
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore
public class RoutingDriverStubTest
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
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config ) )
        {
            // Then
            Set<BoltServerAddress> addresses = driver.routingServers();
            assertThat( addresses, containsInAnyOrder( address(9001), address( 9002 ), address( 9003 ) ) );
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
        BoltServerAddress seed = address( 9001 );

        // When
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config ) )
        {
            // Then
            Set<BoltServerAddress> addresses = driver.routingServers();
            assertThat( addresses, containsInAnyOrder( address(9002), address( 9003 ), address( 9004 ) ) );
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
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config ) )
        {
            Set<BoltServerAddress> servers = driver.routingServers();
            assertThat( servers, hasSize( 0 ) );
            assertFalse( driver.connectionPool().hasAddress( address( 9001 ) ) );
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
        StubServer readServer = StubServer.start( resource( "read_server.script" ), 9005 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
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
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldRoundRobinReadServers() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START two read servers
        StubServer readServer1 = StubServer.start( resource( "read_server.script" ), 9005 );
        StubServer readServer2 = StubServer.start( resource( "read_server.script" ), 9006 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config ) )
        {
            // Run twice, one on each read server
            for ( int i = 0; i < 2; i++ )
            {
                try ( Session session = driver.session( AccessMode.READ ) )
                {
                    assertThat( session.run( "MATCH (n) RETURN n.name" ).list( new Function<Record,String>()
                    {
                        @Override
                        public String apply( Record record )
                        {
                            return record.get( "n.name" ).asString();
                        }
                    } ), equalTo( Arrays.asList( "Bob", "Alice", "Tina" ) ) );
                }
            }
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer1.exitStatus(), equalTo( 0 ) );
        assertThat( readServer2.exitStatus(), equalTo( 0 ) );
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
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
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
        //exception.expectMessage( "Server at 127.0.0.1:9006 is no longer available" );

        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a dead write servers
        StubServer.start( resource( "dead_server.script" ), 9007 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
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
        StubServer writeServer = StubServer.start( resource( "write_server.script" ), 9007 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.WRITE ) )
        {
            session.run( "CREATE (n {name:'Bob'})" );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldRoundRobinWriteSessions() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a write server
        StubServer writeServer1 = StubServer.start( resource( "write_server.script" ), 9007 );
        StubServer writeServer2 = StubServer.start( resource( "write_server.script" ), 9008 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config ) )
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
    public void shouldRememberEndpoints() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a read server
        StubServer readServer = StubServer.start( resource( "read_server.script" ), 9005 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.READ ) )
        {
            session.run( "MATCH (n) RETURN n.name" ).consume();

            assertThat( driver.readServers(), containsInAnyOrder( address( 9005 ), address( 9006 ) ) );
            assertThat( driver.writeServers(), containsInAnyOrder( address( 9007 ), address( 9008 ) ) );
            assertThat( driver.routingServers(), containsInAnyOrder( address( 9001 ), address( 9002 ), address( 9003 ) ) );
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldForgetEndpointsOnFailure() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a read server
        StubServer.start( resource( "dead_server.script" ), 9005 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
        try
        {
            Session session = driver.session( AccessMode.READ );
            session.run( "MATCH (n) RETURN n.name" ).consume();
            session.close();
            fail();
        }
        catch ( SessionExpiredException e )
        {
            //ignore
        }

        assertThat( driver.readServers(), not( hasItem( address( 9005 ) ) ) );
        assertThat( driver.writeServers(), hasSize( 2 ) );
        assertFalse( driver.connectionPool().hasAddress( address( 9005 ) ) );
        driver.close();

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldRediscoverIfNecessaryOnSessionAcquisition()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "rediscover.script" ), 9001 );

        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        //START a read server
        StubServer read = StubServer.start( resource( "empty.script" ), 9005 );

        //On creation we only find ourselves
        RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
        assertThat( driver.routingServers(), containsInAnyOrder( address( 9001 ) ) );
        assertTrue( driver.connectionPool().hasAddress( address( 9001 ) ) );

        //since we have no write nor read servers we must rediscover
        Session session = driver.session( AccessMode.READ );
        assertThat( driver.routingServers(), containsInAnyOrder(address( 9002 ),
               address( 9003 ), address( 9004 ) ) );
        //server told os to forget 9001
        assertFalse( driver.connectionPool().hasAddress( address( 9001 ) ) );
        session.close();
        driver.close();

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( read.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldOnlyGetServersOnce() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "rediscover.script" ), 9001 );

        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        //START a read server
        StubServer read = StubServer.start( resource( "empty.script" ), 9005 );

        //On creation we only find ourselves
        final RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
        assertThat( driver.routingServers(), containsInAnyOrder( address( 9001 ) ) );

        ExecutorService runner = Executors.newFixedThreadPool( 10 );
        for ( int i = 0; i < 10; i++ )
        {
            runner.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    //noinspection EmptyTryBlock
                    try ( Session ignore = driver.session( AccessMode.READ ) )
                    {
                        //empty
                    }

                }
            } );
        }
        runner.awaitTermination( 10, TimeUnit.SECONDS );
        //since we know about less than three servers a rediscover should be triggered
        assertThat( driver.routingServers(), containsInAnyOrder( address( 9002 ), address( 9003 ), address( 9004 ) ) );

        driver.close();

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( read.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldFailOnNonDiscoverableServer() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        //Expect
        exception.expect( ServiceUnavailableException.class );

        // Given
        StubServer.start( resource( "non_discovery_server.script" ), 9001 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        // When
        GraphDatabase.driver( uri, config );
    }

    @Test
    public void shouldFailRandomFailureInGetServers() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        //Expect
        exception.expect( ServiceUnavailableException.class );

        // Given
        StubServer.start( resource( "failed_discovery.script" ), 9001 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        // When
        GraphDatabase.driver( uri, config );
    }

    @Test
    public void shouldHandleLeaderSwitchWhenWriting()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "acquire_endpoints.script" ), 9001 );

        //START a write server that doesn't accept writes
        StubServer.start( resource( "not_able_to_write_server.script" ), 9007 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
        boolean failed = false;
        try ( Session session = driver.session( AccessMode.WRITE ) )
        {
            assertThat( driver.writeServers(), hasItem(address( 9007 ) ) );
            assertThat( driver.writeServers(), hasItem( address( 9008 ) ) );
            session.run( "CREATE ()" ).consume();
        }
        catch ( SessionExpiredException e )
        {
            failed = true;
            assertThat( e.getMessage(), equalTo( "Server at 127.0.0.1:9007 no longer accepts writes" ) );
        }
        assertTrue( failed );
        assertThat( driver.writeServers(), not( hasItem( address( 9007 ) ) ) );
        assertThat( driver.writeServers(), hasItem( address( 9008 ) ) );
        assertTrue( driver.connectionPool().hasAddress( address( 9007 ) ) );

        driver.close();
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldRediscoverOnExpiry() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "expire.script" ), 9001 );

        //START a read server
        StubServer readServer = StubServer.start( resource( "empty.script" ), 9005 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
        assertThat(driver.routingServers(), contains(address( 9001 )));
        assertThat(driver.readServers(), contains(address( 9002 )));
        assertThat(driver.writeServers(), contains(address( 9003 )));

        //On acquisition we should update our view
        Session session = driver.session( AccessMode.READ );
        assertThat(driver.routingServers(), contains(address( 9004 )));
        assertThat(driver.readServers(), contains(address( 9005 )));
        assertThat(driver.writeServers(), contains(address( 9006 )));
        session.close();
        driver.close();
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldNotPutBackPurgedConnection() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( resource( "not_reuse_connection.script" ), 9001 );

        //START servers
        StubServer readServer = StubServer.start( resource( "empty.script" ), 9002 );
        StubServer writeServer1 = StubServer.start( resource( "dead_server.script" ), 9003 );
        StubServer writeServer2 = StubServer.start( resource( "empty.script" ), 9006 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );


        //Open both a read and a write session
        Session readSession = driver.session( AccessMode.READ );
        Session writeSession = driver.session( AccessMode.WRITE );

        try
        {
            writeSession.run( "MATCH (n) RETURN n.name" );
            writeSession.close();
            fail();
        }
        catch (SessionExpiredException e)
        {
            //ignore
        }
        //We now lost all write servers
        assertThat(driver.writeServers(), hasSize( 0 ));

        //reacquiring will trow out the current read server at 9002
        writeSession = driver.session( AccessMode.WRITE );

        assertThat(driver.routingServers(), contains(address( 9004 )));
        assertThat(driver.readServers(), contains(address( 9005 )));
        assertThat(driver.writeServers(), contains(address( 9006 )));
        assertFalse(driver.connectionPool().hasAddress(address( 9002 ) ));

        // now we close the read session and the connection should not be put
        // back to the pool
        Connection connection = ((RoutingNetworkSession) readSession).connection;
        assertTrue( connection.isOpen() );
        readSession.close();
        assertFalse( connection.isOpen() );
        assertFalse(driver.connectionPool().hasAddress(address( 9002 ) ));
        writeSession.close();

        driver.close();

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
        assertThat( readServer.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer1.exitStatus(), equalTo( 0 ) );
        assertThat( writeServer2.exitStatus(), equalTo( 0 ) );
    }

    String resource( String fileName )
    {
        URL resource = RoutingDriverStubTest.class.getClassLoader().getResource( fileName );
        if ( resource == null )
        {
            fail( fileName + " does not exists" );
        }
        return resource.getFile();
    }

    private BoltServerAddress address( int port )
    {
        return new BoltServerAddress( "127.0.0.1", port );
    }
}
