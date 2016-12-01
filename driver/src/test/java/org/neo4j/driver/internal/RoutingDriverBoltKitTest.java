/*
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RoutingDriverBoltKitTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final Config config = Config.build()
            .withEncryptionLevel( Config.EncryptionLevel.NONE )
            .withLogging( new ConsoleLogging( Level.INFO ) ).toConfig();

    @Test
    public void shouldHandleAcquireReadSession() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a read server
        StubServer readServer = StubServer.start( "read_server.script", 9005 );
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
    public void shouldHandleAcquireReadSessionPlusTransaction()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a read server
        StubServer readServer = StubServer.start( "read_server.script", 9005 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.READ );
              Transaction tx = session.beginTransaction() )
        {
            List<String> result = tx.run( "MATCH (n) RETURN n.name" ).list( new Function<Record,String>()
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
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START two read servers
        StubServer readServer1 = StubServer.start( "read_server.script", 9005 );
        StubServer readServer2 = StubServer.start( "read_server.script", 9006 );
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
    public void shouldRoundRobinReadServersWhenUsingTransaction()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START two read servers
        StubServer readServer1 = StubServer.start( "read_server.script", 9005 );
        StubServer readServer2 = StubServer.start( "read_server.script", 9006 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config ) )
        {
            // Run twice, one on each read server
            for ( int i = 0; i < 2; i++ )
            {
                try ( Session session = driver.session( AccessMode.READ );
                      Transaction tx = session.beginTransaction() )
                {
                    assertThat( tx.run( "MATCH (n) RETURN n.name" ).list( new Function<Record,String>()
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
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a read server
        StubServer.start( "dead_server.script", 9005 );
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
    public void shouldThrowSessionExpiredIfReadServerDisappearsWhenUsingTransaction()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        //Expect
        exception.expect( SessionExpiredException.class );
        exception.expectMessage( "Server at 127.0.0.1:9005 is no longer available" );

        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a read server
        StubServer.start( "dead_server.script", 9005 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.READ );
              Transaction tx = session.beginTransaction() )
        {
            tx.run( "MATCH (n) RETURN n.name" );
            tx.success();
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
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a dead write servers
        StubServer.start( "dead_server.script", 9007 );
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
    public void shouldThrowSessionExpiredIfWriteServerDisappearsWhenUsingTransaction()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        //Expect
        exception.expect( SessionExpiredException.class );
        //exception.expectMessage( "Server at 127.0.0.1:9006 is no longer available" );

        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a dead write servers
        StubServer.start( "dead_server.script", 9007 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.WRITE );
              Transaction tx = session.beginTransaction() )
        {
            tx.run( "MATCH (n) RETURN n.name" ).consume();
            tx.success();
        }
        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @Test
    public void shouldHandleAcquireWriteSession() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a write server
        StubServer writeServer = StubServer.start( "write_server.script", 9007 );
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
    public void shouldHandleAcquireWriteSessionAndTransaction()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a write server
        StubServer writeServer = StubServer.start( "write_server.script", 9007 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
              Session session = driver.session( AccessMode.WRITE );
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
    public void shouldRoundRobinWriteSessions() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a write server
        StubServer writeServer1 = StubServer.start( "write_server.script", 9007 );
        StubServer writeServer2 = StubServer.start( "write_server.script", 9008 );
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
    public void shouldRoundRobinWriteSessionsInTransaction() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a write server
        StubServer writeServer1 = StubServer.start( "write_server.script", 9007 );
        StubServer writeServer2 = StubServer.start( "write_server.script", 9008 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        try ( RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config ) )
        {
            for ( int i = 0; i < 2; i++ )
            {
                try ( Session session = driver.session();
                      Transaction tx = session.beginTransaction())
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
    public void shouldFailOnNonDiscoverableServer() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        //Expect
        exception.expect( ServiceUnavailableException.class );

        // Given
        StubServer.start( "non_discovery_server.script", 9001 );
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
        StubServer.start( "failed_discovery.script", 9001 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        // When
        GraphDatabase.driver( uri, config );
    }

    @Test
    public void shouldHandleLeaderSwitchWhenWriting()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a write server that doesn't accept writes
        StubServer.start( "not_able_to_write_server.script", 9007 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
        boolean failed = false;
        try ( Session session = driver.session( AccessMode.WRITE ) )
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
    public void shouldHandleLeaderSwitchWhenWritingWithoutConsuming()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a write server that doesn't accept writes
        StubServer.start( "not_able_to_write_server.script", 9007 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
        boolean failed = false;
        try ( Session session = driver.session( AccessMode.WRITE ) )
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
    public void shouldHandleLeaderSwitchWhenWritingInTransaction()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a write server that doesn't accept writes
        StubServer.start( "not_able_to_write_server.script", 9007 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
        boolean failed = false;
        try ( Session session = driver.session( AccessMode.WRITE );
              Transaction tx = session.beginTransaction() )
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
    public void shouldHandleLeaderSwitchWhenWritingInTransactionWithoutConsuming()
            throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "acquire_endpoints.script", 9001 );

        //START a write server that doesn't accept writes
        StubServer.start( "not_able_to_write_server.script", 9007 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );
        RoutingDriver driver = (RoutingDriver) GraphDatabase.driver( uri, config );
        boolean failed = false;
        try ( Session session = driver.session( AccessMode.WRITE );
              Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE ()" );
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
}
