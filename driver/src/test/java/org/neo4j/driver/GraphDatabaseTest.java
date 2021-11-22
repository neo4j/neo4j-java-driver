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
package org.neo4j.driver;

import io.netty.util.concurrent.EventExecutorGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.List;

import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.InternalDriver;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.util.TestUtil;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Logging.none;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

class GraphDatabaseTest
{
    private static final Config INSECURE_CONFIG = Config.builder()
                                                        .withoutEncryption()
                                                        .withLogging( none() )
                                                        .build();

    @Test
    void throwsWhenBoltSchemeUsedWithRoutingParams()
    {
        assertThrows( IllegalArgumentException.class, () -> GraphDatabase.driver( "bolt://localhost:7687/?policy=my_policy" ) );
    }

    @Test
    void shouldLogWhenUnableToCreateRoutingDriver()
    {
        Logging logging = mock( Logging.class );
        Logger logger = mock( Logger.class );
        when( logging.getLog( any( Class.class ) ) ).thenReturn( logger );
        InternalDriver driver = mock( InternalDriver.class );
        doThrow( ServiceUnavailableException.class ).when( driver ).verifyConnectivity();
        DriverFactory driverFactory = new MockSupplyingDriverFactory( driver );
        Config config = Config.builder()
                              .withLogging( logging )
                              .build();

        List<URI> routingUris = asList(
                URI.create( "neo4j://localhost:9001" ),
                URI.create( "neo4j://localhost:9002" ) );

        assertThrows( ServiceUnavailableException.class, () -> GraphDatabase.routingDriver( routingUris, AuthTokens.none(), config, driverFactory ) );

        verify( logger ).warn( eq( "Unable to create routing driver for URI: neo4j://localhost:9001" ),
                any( Throwable.class ) );

        verify( logger ).warn( eq( "Unable to create routing driver for URI: neo4j://localhost:9002" ),
                any( Throwable.class ) );
    }

    @Test
    void shouldRespondToInterruptsWhenConnectingToUnresponsiveServer() throws Exception
    {
        try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
        {
            // setup other thread to interrupt current thread when it blocks
            TestUtil.interruptWhenInWaitingState( Thread.currentThread() );

            final Driver driver = GraphDatabase.driver( "bolt://localhost:" + serverSocket.getLocalPort() );
            try
            {
                assertThrows( ServiceUnavailableException.class, driver::verifyConnectivity );
            }
            finally
            {
                // clear interrupted flag
                Thread.interrupted();
            }
        }
    }

    @Test
    void shouldPrintNiceErrorWhenConnectingToUnresponsiveServer() throws Exception
    {
        int localPort = -1;
        try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
        {
            localPort = serverSocket.getLocalPort();
        }
        final Driver driver = GraphDatabase.driver( "bolt://localhost:" + localPort, INSECURE_CONFIG );
        final ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, driver::verifyConnectivity );
        assertThat( error.getMessage(), containsString( "Unable to connect to" ) );
    }

    @Test
    void shouldPrintNiceRoutingErrorWhenConnectingToUnresponsiveServer() throws Exception
    {
        int localPort = -1;
        try ( ServerSocket serverSocket = new ServerSocket( 0 ) )
        {
            localPort = serverSocket.getLocalPort();
        }
        final Driver driver = GraphDatabase.driver( "neo4j://localhost:" + localPort, INSECURE_CONFIG );
        final ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, driver::verifyConnectivity );
        error.printStackTrace();
        assertThat( error.getMessage(), containsString( "Unable to connect to" ) );
    }

    @Test
    void shouldFailToCreateUnencryptedDriverWhenServerDoesNotRespond() throws IOException
    {
        testFailureWhenServerDoesNotRespond( false );
    }

    @Test
    void shouldFailToCreateEncryptedDriverWhenServerDoesNotRespond() throws IOException
    {
        testFailureWhenServerDoesNotRespond( true );
    }

    private static void testFailureWhenServerDoesNotRespond( boolean encrypted ) throws IOException
    {
        try ( ServerSocket server = new ServerSocket( 0 ) ) // server that accepts connections but does not reply
        {
            int connectionTimeoutMillis = 1_000;
            Config config = createConfig( encrypted, connectionTimeoutMillis );
            final Driver driver = GraphDatabase.driver( URI.create( "bolt://localhost:" + server.getLocalPort() ), config );

            ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, driver::verifyConnectivity );
            assertEquals( e.getMessage(), "Unable to establish connection in " + connectionTimeoutMillis + "ms" );
        }
    }

    private static Config createConfig( boolean encrypted, int timeoutMillis )
    {
        Config.ConfigBuilder configBuilder = Config.builder()
                .withConnectionTimeout( timeoutMillis, MILLISECONDS )
                .withLogging( DEV_NULL_LOGGING );

        if ( encrypted )
        {
            configBuilder.withEncryption();
        }
        else
        {
            configBuilder.withoutEncryption();
        }

        return configBuilder.build();
    }

    private static class MockSupplyingDriverFactory extends DriverFactory
    {
        private final InternalDriver driver;

        private MockSupplyingDriverFactory( InternalDriver driver )
        {
            this.driver = driver;
        }

        @Override
        protected InternalDriver createRoutingDriver( SecurityPlan securityPlan, BoltServerAddress address, ConnectionPool connectionPool,
                                                      EventExecutorGroup eventExecutorGroup, RoutingSettings routingSettings, RetryLogic retryLogic,
                                                      MetricsProvider metricsProvider, Config config )
        {
            return driver;
        }
    }
}
