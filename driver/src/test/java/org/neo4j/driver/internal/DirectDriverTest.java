/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.StubServer;
import org.neo4j.driver.v1.util.TestNeo4j;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.util.Matchers.directDriverWithAddress;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.StubServer.INSECURE_CONFIG;

public class DirectDriverTest
{
    @ClassRule
    public static final TestNeo4j neo4j = new TestNeo4j();

    private Driver driver;

    @After
    public void closeDriver() throws Exception
    {
        if ( driver != null )
        {
            driver.close();
        }
    }

    @Test
    public void shouldUseDefaultPortIfMissing()
    {
        // Given
        URI uri = URI.create( "bolt://localhost" );

        // When
        driver = GraphDatabase.driver( uri, neo4j.authToken() );

        // Then
        assertThat( driver, is( directDriverWithAddress( LOCAL_DEFAULT ) ) );
    }

    @Test
    public void shouldAllowIPv6Address()
    {
        // Given
        URI uri = URI.create( "bolt://[::1]" );
        BoltServerAddress address = BoltServerAddress.from( uri );

        // When
        driver = GraphDatabase.driver( uri, neo4j.authToken() );

        // Then
        assertThat( driver, is( directDriverWithAddress( address ) ) );
    }

    @Test
    public void shouldRejectInvalidAddress()
    {
        // Given
        URI uri = URI.create( "*" );

        // When & Then
        try
        {
            driver = GraphDatabase.driver( uri, neo4j.authToken() );
            fail("Expecting error for wrong uri");
        }
        catch( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), equalTo( "Invalid URI format `*`" ) );
        }
    }

    @Test
    public void shouldRegisterSingleServer()
    {
        // Given
        URI uri = URI.create( "bolt://localhost:7687" );
        BoltServerAddress address = BoltServerAddress.from( uri );

        // When
        driver = GraphDatabase.driver( uri, neo4j.authToken() );

        // Then
        assertThat( driver, is( directDriverWithAddress( address ) ) );
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
