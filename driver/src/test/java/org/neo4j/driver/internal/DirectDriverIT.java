/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.util.DatabaseExtension;
import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.util.Matchers.directDriverWithAddress;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.StubServer.INSECURE_CONFIG;

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
    void shouldUseDefaultPortIfMissing()
    {
        // Given
        URI uri = URI.create( "bolt://localhost" );

        // When
        driver = GraphDatabase.driver( uri, neo4j.authToken() );

        // Then
        assertThat( driver, is( directDriverWithAddress( LOCAL_DEFAULT ) ) );
    }

    @Test
    void shouldAllowIPv6Address()
    {
        assumeTrue( supportsListenAddressConfiguration( neo4j ) );

        // Given
        URI uri = URI.create( "bolt://[::1]" );
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
        assertThat( e.getMessage(), equalTo( "Invalid address format `*`" ) );
    }

    @Test
    void shouldRegisterSingleServer()
    {
        // Given
        URI uri = URI.create( "bolt://localhost:7687" );
        BoltServerAddress address = new BoltServerAddress( uri );

        // When
        driver = GraphDatabase.driver( uri, neo4j.authToken() );

        // Then
        assertThat( driver, is( directDriverWithAddress( address ) ) );
    }

    @Test
    void shouldBeAbleRunCypher() throws Exception
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

    @Test
    void shouldSendMultipleBookmarks() throws Exception
    {
        StubServer server = StubServer.start( "multiple_bookmarks.script", 9001 );

        List<String> bookmarks = Arrays.asList( "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx29",
                "neo4j:bookmark:v1:tx94", "neo4j:bookmark:v1:tx56", "neo4j:bookmark:v1:tx16",
                "neo4j:bookmark:v1:tx68" );

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:9001", INSECURE_CONFIG );
              Session session = driver.session( bookmarks ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (n {name:'Bob'})" );
                tx.success();
            }

            assertEquals( "neo4j:bookmark:v1:tx95", session.lastBookmark() );
        }
        finally
        {
            assertEquals( 0, server.exitStatus() );
        }
    }

    /**
     * Check if running test neo4j instance supports {@value org.neo4j.driver.v1.util.Neo4jSettings#LISTEN_ADDR}
     * configuration option. Only 3.1+ versions support it.
     *
     * @param neo4j the test neo4j instance to check.
     * @return {@code true} if given test neo4j supports config option, {@code false} otherwise.
     */
    private static boolean supportsListenAddressConfiguration( DatabaseExtension neo4j )
    {
        ServerVersion version = ServerVersion.version( neo4j.driver() );
        return version.greaterThanOrEqual( ServerVersion.v3_1_0 );
    }
}
