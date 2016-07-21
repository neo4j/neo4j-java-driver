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

package org.neo4j.driver.v1;

import org.junit.Test;
import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.internal.net.BoltServerAddress;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class ClusterDriverTest
{
    private static final Config config = Config.build().withLogging( new ConsoleLogging( Level.INFO ) ).toConfig();

    private Process startStubServer( int port, String script ) throws IOException
    {
        List<String> command = new ArrayList<>();
        command.addAll( singletonList( "boltstub" ) );
        command.addAll( asList( Integer.toString( port ), script ) );
        ProcessBuilder processBuilder = new ProcessBuilder().inheritIO();
        return processBuilder.command( command ).start();
    }

    @Test
    public void shouldDiscoverServers() throws IOException
    {
        // Given
        startStubServer( 9001, "../driver/src/test/resources/discovery.script" );
        URI uri = URI.create( "bolt+discovery://127.0.0.1:9001" );

        // When
        try ( Driver driver = GraphDatabase.driver( uri, config ) )
        {
            // Then
            List<BoltServerAddress> addresses = driver.servers();
            assertThat( addresses.size(), equalTo( 3 ) );
            assertThat( addresses.get( 0 ), equalTo( new BoltServerAddress( "127.0.0.1", 9001 ) ) );
            assertThat( addresses.get( 1 ), equalTo( new BoltServerAddress( "127.0.0.1", 9002 ) ) );
            assertThat( addresses.get( 2 ), equalTo( new BoltServerAddress( "127.0.0.1", 9003 ) ) );
        }

    }

}