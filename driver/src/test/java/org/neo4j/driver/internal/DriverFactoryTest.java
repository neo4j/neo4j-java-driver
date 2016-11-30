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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.v1.Config.defaultConfig;

@RunWith( Parameterized.class )
public class DriverFactoryTest
{
    @Parameter
    public URI uri;

    @Parameters( name = "{0}" )
    public static List<URI> uris()
    {
        return Arrays.asList(
                URI.create( "bolt://localhost:7687" ),
                URI.create( "bolt+routing://localhost:7687" )
        );
    }

    @Test
    public void connectionPoolClosedWhenDriverCreationFails() throws Exception
    {
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        DriverFactory factory = new ThrowingDriverFactory( connectionPool );

        try
        {
            factory.newInstance( uri, dummyAuthToken(), dummyRoutingSettings(), defaultConfig() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
        }
        verify( connectionPool ).close();
    }

    @Test
    public void connectionPoolCloseExceptionIsSupressedWhenDriverCreationFails() throws Exception
    {
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        RuntimeException poolCloseError = new RuntimeException( "Pool close error" );
        doThrow( poolCloseError ).when( connectionPool ).close();

        DriverFactory factory = new ThrowingDriverFactory( connectionPool );

        try
        {
            factory.newInstance( uri, dummyAuthToken(), dummyRoutingSettings(), defaultConfig() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( UnsupportedOperationException.class ) );
            assertArrayEquals( new Throwable[]{poolCloseError}, e.getSuppressed() );
        }
        verify( connectionPool ).close();
    }

    private static AuthToken dummyAuthToken()
    {
        return AuthTokens.basic( "neo4j", "neo4j" );
    }

    private static RoutingSettings dummyRoutingSettings()
    {
        return new RoutingSettings( 42, 42 );
    }

    private static class ThrowingDriverFactory extends DriverFactory
    {
        final ConnectionPool connectionPool;

        ThrowingDriverFactory( ConnectionPool connectionPool )
        {
            this.connectionPool = connectionPool;
        }

        @Override
        DirectDriver createDirectDriver( BoltServerAddress address, ConnectionPool connectionPool, Config config,
                SecurityPlan securityPlan, SessionFactory sessionFactory )
        {
            throw new UnsupportedOperationException( "Can't create direct driver" );
        }

        @Override
        RoutingDriver createRoutingDriver( BoltServerAddress address, ConnectionPool connectionPool, Config config,
                RoutingSettings routingSettings, SecurityPlan securityPlan, SessionFactory sessionFactory )
        {
            throw new UnsupportedOperationException( "Can't create routing driver" );
        }

        @Override
        ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Config config )
        {
            return connectionPool;
        }
    }
}
