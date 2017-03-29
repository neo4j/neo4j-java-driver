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
package org.neo4j.driver.v1;

import org.junit.Test;

import java.io.File;
import java.net.URI;

import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.util.Matchers.clusterDriver;
import static org.neo4j.driver.internal.util.Matchers.directDriver;
import static org.neo4j.driver.v1.Config.EncryptionLevel.REQUIRED;
import static org.neo4j.driver.v1.Config.TrustStrategy.trustOnFirstUse;
import static org.neo4j.driver.v1.util.StubServer.INSECURE_CONFIG;

public class GraphDatabaseTest
{
    @Test
    public void boltSchemeShouldInstantiateDirectDriver()
    {
        // Given
        URI uri = URI.create( "bolt://localhost:7687" );

        // When
        Driver driver = GraphDatabase.driver( uri );

        // Then
        assertThat( driver, is( directDriver() ) );
    }

    @Test
    public void boltPlusDiscoverySchemeShouldInstantiateClusterDriver() throws Exception
    {
        // Given
        StubServer server = StubServer.start( "discover_servers.script", 9001 );
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        // When
        Driver driver = GraphDatabase.driver( uri, INSECURE_CONFIG );

        // Then
        assertThat( driver, is( clusterDriver() ) );

        // Finally
        driver.close();
        assertThat( server.exitStatus(), equalTo( 0 ) );
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void boltPlusDiscoverySchemeShouldNotSupportTrustOnFirstUse()
    {
        URI uri = URI.create( "bolt+routing://127.0.0.1:9001" );

        Config config = Config.build()
                .withEncryptionLevel( REQUIRED )
                .withTrustStrategy( trustOnFirstUse( new File( "./known_hosts" ) ) )
                .toConfig();

        try
        {
            GraphDatabase.driver( uri, config );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void throwsWhenBoltSchemeUsedWithRoutingParams()
    {
        try
        {
            GraphDatabase.driver( "bolt://localhost:7687/?policy=my_policy" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }
}
