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
package org.neo4j.driver.internal.net;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.net.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.security.SecurityPlan.insecure;

public class SocketConnectorTest
{
    @Test
    public void connectCreatesConnection()
    {
        ConnectionSettings settings = new ConnectionSettings( basicAuthToken() );
        SocketConnector connector = new TestSocketConnector( settings, insecure(), loggingMock() );

        Connection connection = connector.connect( LOCAL_DEFAULT );

        assertThat( connection, instanceOf( ConcurrencyGuardingConnection.class ) );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void connectSendsInit()
    {
        String userAgent = "agentSmith";
        ConnectionSettings settings = new ConnectionSettings( basicAuthToken(), userAgent );
        TestSocketConnector connector = new TestSocketConnector( settings, insecure(), loggingMock() );

        connector.connect( LOCAL_DEFAULT );

        assertEquals( 1, connector.createConnections.size() );
        Connection connection = connector.createConnections.get( 0 );
        verify( connection ).init( eq( userAgent ), any( Map.class ) );
    }

    @Test
    public void connectThrowsForUnknownAuthToken()
    {
        ConnectionSettings settings = new ConnectionSettings( mock( AuthToken.class ) );
        TestSocketConnector connector = new TestSocketConnector( settings, insecure(), loggingMock() );

        try
        {
            connector.connect( LOCAL_DEFAULT );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ClientException.class ) );
        }
    }

    private static Logging loggingMock()
    {
        return mock( Logging.class, RETURNS_MOCKS );
    }

    private static AuthToken basicAuthToken()
    {
        return AuthTokens.basic( "neo4j", "neo4j" );
    }

    private static class TestSocketConnector extends SocketConnector
    {
        final List<Connection> createConnections = new CopyOnWriteArrayList<>();

        TestSocketConnector( ConnectionSettings settings, SecurityPlan securityPlan, Logging logging )
        {
            super( settings, securityPlan, logging );
        }

        @Override
        Connection createConnection( BoltServerAddress address, SecurityPlan securityPlan, Logging logging )
        {
            Connection connection = mock( Connection.class );
            when( connection.boltServerAddress() ).thenReturn( address );
            createConnections.add( connection );
            return connection;
        }
    }
}
