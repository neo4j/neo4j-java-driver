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

import org.junit.Test;

import java.net.URI;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.StubServer.INSECURE_CONFIG;

public class DirectDriverTest
{
    @Test
    public void shouldUseDefaultPortIfMissing()
    {
        // Given
        URI uri = URI.create( "bolt://localhost:7687" );

        // When
        DirectDriver driver = (DirectDriver) GraphDatabase.driver( uri );

        // Then
        BoltServerAddress address = driver.server();
        assertThat( address.port(), equalTo( BoltServerAddress.DEFAULT_PORT ) );
    }

    @Test
    public void shouldRegisterSingleServer()
    {
        // Given
        URI uri = URI.create( "bolt://localhost:7687" );
        BoltServerAddress address = BoltServerAddress.from( uri );

        // When
        DirectDriver driver = (DirectDriver) GraphDatabase.driver( uri );

        // Then
        BoltServerAddress driverAddress = driver.server();
        assertThat( driverAddress, equalTo( address ));

    }

    @Test
    public void shouldBeAbleRunCypher() throws Exception
    {
        // Given
        StubServer server = StubServer.start( "return_x.script", 9001 );
        URI uri = URI.create( "bolt://127.0.0.1:9001" );
        int x;

        // When
        try ( Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG ) )
        {
            try ( Session session = driver.session() )
            {
                Record record = session.run( "RETURN {x}", parameters( "x", 1 ) ).single();
                x = record.get( 0 ).asInt();
            }
        }

        // Then
        assertThat( x, equalTo( 1 ) );

        // Finally
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }
}
