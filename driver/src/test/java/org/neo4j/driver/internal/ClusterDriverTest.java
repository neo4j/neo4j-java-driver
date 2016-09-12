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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.logging.Level;

import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class ClusterDriverTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static final Config config = Config.build().withLogging( new ConsoleLogging( Level.INFO ) ).toConfig();

    @Test
    public void shouldDiscoverServers() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "../driver/src/test/resources/discover_servers.script", 9001 );
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

    @Test
    public void shouldDiscoverNewServers() throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // Given
        StubServer server = StubServer.start( "../driver/src/test/resources/discover_new_servers.script", 9001 );
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
}