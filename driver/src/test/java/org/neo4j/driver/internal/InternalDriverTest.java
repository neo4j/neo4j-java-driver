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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.Config;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.metrics.InternalMetrics;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.Clock;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.metrics.MetricsProvider.METRICS_DISABLED_PROVIDER;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.util.TestUtil.await;

class InternalDriverTest
{
    @Test
    void shouldCloseSessionFactory()
    {
        SessionFactory sessionFactory = sessionFactoryMock();
        InternalDriver driver = newDriver( sessionFactory );

        assertNull( await( driver.closeAsync() ) );
        verify( sessionFactory ).close();
    }

    @Test
    void shouldNotCloseSessionFactoryMultipleTimes()
    {
        SessionFactory sessionFactory = sessionFactoryMock();
        InternalDriver driver = newDriver( sessionFactory );

        assertNull( await( driver.closeAsync() ) );
        assertNull( await( driver.closeAsync() ) );
        assertNull( await( driver.closeAsync() ) );

        verify( sessionFactory ).close();
    }

    @Test
    void shouldVerifyConnectivity()
    {
        SessionFactory sessionFactory = sessionFactoryMock();
        CompletableFuture<Void> connectivityStage = completedWithNull();
        when( sessionFactory.verifyConnectivity() ).thenReturn( connectivityStage );

        InternalDriver driver = newDriver( sessionFactory );

        assertEquals( connectivityStage, driver.verifyConnectivityAsync() );
    }

    @Test
    void shouldThrowWhenUnableToVerifyConnectivity()
    {
        SessionFactory sessionFactory = mock( SessionFactory.class );
        ServiceUnavailableException error = new ServiceUnavailableException( "Hello" );
        when( sessionFactory.verifyConnectivity() ).thenReturn( failedFuture( error ) );
        InternalDriver driver = newDriver( sessionFactory );

        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> await( driver.verifyConnectivityAsync() ) );
        assertEquals( e.getMessage(), "Hello" );
    }

    @Test
    void shouldThrowClientExceptionIfMetricsNotEnabled() throws Throwable
    {
        // Given
        InternalDriver driver = newDriver( false );

        // When
        ClientException error = assertThrows( ClientException.class, driver::metrics );

        // Then
        assertTrue( error.getMessage().contains( "Driver metrics not enabled." ) );
    }

    @Test
    void shouldReturnMetricsIfMetricsEnabled() throws Throwable
    {
        // Given
        InternalDriver driver = newDriver( true );

        // When
        Metrics metrics = driver.metrics();

        // Then we shall have no problem to get the metrics
        assertTrue( metrics instanceof InternalMetrics );
    }

    private static InternalDriver newDriver( SessionFactory sessionFactory )
    {
        return new InternalDriver( SecurityPlanImpl.insecure(), sessionFactory, METRICS_DISABLED_PROVIDER, DEV_NULL_LOGGING );
    }

    private static SessionFactory sessionFactoryMock()
    {
        SessionFactory sessionFactory = mock( SessionFactory.class );
        when( sessionFactory.close() ).thenReturn( completedWithNull() );
        return sessionFactory;
    }

    private static InternalDriver newDriver( boolean isMetricsEnabled )
    {
        SessionFactory sessionFactory = sessionFactoryMock();
        Config config = Config.defaultConfig();
        if ( isMetricsEnabled )
        {
            config = Config.builder().withDriverMetrics().build();
        }

        MetricsProvider metricsProvider = DriverFactory.createDriverMetrics( config, Clock.SYSTEM );
        return new InternalDriver( SecurityPlanImpl.insecure(), sessionFactory, metricsProvider, DEV_NULL_LOGGING );
    }
}
