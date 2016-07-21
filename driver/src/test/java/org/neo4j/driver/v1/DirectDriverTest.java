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

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import java.net.URI;
import java.util.Collection;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

public class DirectDriverTest
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldUseDefaultPortIfMissing()
    {
        // Given
        URI uri = URI.create( "bolt://localhost" );

        // When
        Driver driver = GraphDatabase.driver( uri );

        // Then
        Collection<BoltServerAddress> addresses = driver.servers();
        assertThat( addresses.size(), equalTo( 1 ) );
        for ( BoltServerAddress address : addresses )
        {
            assertThat( address.port(), equalTo( BoltServerAddress.DEFAULT_PORT ) );
        }

    }

    @Test
    public void shouldRegisterSingleServer()
    {
        // Given
        URI uri = URI.create( "bolt://localhost:7687" );
        BoltServerAddress address = BoltServerAddress.from( uri );

        // When
        Driver driver = GraphDatabase.driver( uri );

        // Then
        Collection<BoltServerAddress> addresses = driver.servers();
        assertThat( addresses.size(), equalTo( 1 ) );
        assertThat( addresses.contains( address ), equalTo( true ) );

    }

    @Test
    public void shouldBeAbleRunCypher()
    {
        // Given
        URI uri = URI.create( "bolt://localhost:7687" );
        int x;

        // When
        try ( Driver driver = GraphDatabase.driver( uri ) )
        {
            try ( Session session = driver.session() )
            {
                Record record = session.run( "RETURN {x}", parameters( "x", 1 ) ).single();
                x = record.get( 0 ).asInt();
            }
        }

        // Then
        assertThat( x, equalTo( 1 ) );

    }

}