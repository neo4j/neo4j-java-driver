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
package org.neo4j.driver.internal.pool;

import org.junit.Test;

import java.net.URI;
import java.util.Collections;

import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Connector;
import org.neo4j.driver.internal.util.BoltServerAddress;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Logging;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class InternalConnectionPoolTest
{

    @Test
    public void shouldAcquireAndRelease() throws Throwable
    {
        // Given
        BoltServerAddress address = BoltServerAddress.LOCAL_DEFAULT;
        Connector connector = connector( "bolt" );
        Config config = Config.defaultConfig();
        SecurityPlan securityPlan = SecurityPlan.insecure();
        PoolSettings poolSettings = PoolSettings.defaultSettings();
        InternalConnectionPool pool = new InternalConnectionPool(
                connector, Clock.SYSTEM, securityPlan, poolSettings, config.logging() );

        Connection conn = pool.acquire( address );
        conn.close();

        // When
        Connection acquired = pool.acquire( address );

        // Then
        verify( connector, times( 1 ) ).connect( address, securityPlan, config.logging() );
        assertThat( acquired, equalTo(conn) );
    }

    private Connector connector( String scheme )
    {
        Connector mock = mock( Connector.class );
        when( mock.supportedSchemes() ).thenReturn( Collections.singletonList( scheme ) );
        when( mock.connect( any( BoltServerAddress.class ), any( SecurityPlan.class ), any( Logging.class ) ) ).thenReturn( mock(
                Connection.class
        ) );
        return mock;
    }
}
