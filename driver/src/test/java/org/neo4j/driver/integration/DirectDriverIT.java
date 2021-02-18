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

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Matchers.directDriverWithAddress;

@ParallelizableIT
class DirectDriverIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;

    @AfterEach
    void closeDriver()
    {
        if ( driver != null )
        {
            driver.close();
        }
    }

    @Test
    void shouldAllowIPv6Address()
    {
        // Given
        URI uri = URI.create( "bolt://[::1]:" + neo4j.boltPort() );
        BoltServerAddress address = new BoltServerAddress( uri );

        // When
        driver = GraphDatabase.driver( uri, neo4j.authToken() );

        // Then
        assertThat( driver, is( directDriverWithAddress( address ) ) );
    }

    @Test
    void shouldRejectInvalidAddress()
    {
        // Given
        URI uri = URI.create( "*" );

        // When & Then
        IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> GraphDatabase.driver( uri, neo4j.authToken() ) );
        assertThat( e.getMessage(), equalTo( "Scheme must not be null" ) );
    }

    @Test
    void shouldRegisterSingleServer()
    {
        // Given
        URI uri = neo4j.uri();
        BoltServerAddress address = new BoltServerAddress( uri );

        // When
        driver = GraphDatabase.driver( uri, neo4j.authToken() );

        // Then
        assertThat( driver, is( directDriverWithAddress( address ) ) );
    }

    @Test
    void shouldConnectIPv6Uri()
    {
        // Given
        try ( Driver driver = GraphDatabase.driver( "bolt://[::1]:" + neo4j.boltPort(), neo4j.authToken() );
              Session session = driver.session() )
        {
            // When
            Result result = session.run( "RETURN 1" );

            // Then
            assertThat( result.single().get( 0 ).asInt(), CoreMatchers.equalTo( 1 ) );
        }
    }
}
