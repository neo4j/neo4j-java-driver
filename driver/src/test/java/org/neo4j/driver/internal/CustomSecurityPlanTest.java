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

import io.netty.bootstrap.Bootstrap;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ConnectionPool;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class CustomSecurityPlanTest
{
    @Test
    void testCustomSecurityPlanUsed()
    {
        SecurityPlanCapturingDriverFactory driverFactory = new SecurityPlanCapturingDriverFactory();

        SecurityPlan securityPlan = mock( SecurityPlan.class );

        driverFactory.newInstance( 
                URI.create( "neo4j://somewhere:1234" ),
                AuthTokens.none(),
                RoutingSettings.DEFAULT,
                RetrySettings.DEFAULT,
                Config.defaultConfig(),
                null,
                securityPlan
        );

        assertFalse( driverFactory.capturedSecurityPlans.isEmpty() );
        assertTrue( driverFactory.capturedSecurityPlans.stream().allMatch( capturePlan -> capturePlan == securityPlan ) );
    }
    
    private static class SecurityPlanCapturingDriverFactory extends DriverFactory
    {
        List<SecurityPlan> capturedSecurityPlans = new ArrayList<>();

        @Override
        protected InternalDriver createDriver( SecurityPlan securityPlan, SessionFactory sessionFactory, MetricsProvider metricsProvider, Config config )
        {
            capturedSecurityPlans.add( securityPlan );
            return super.createDriver( securityPlan, sessionFactory, metricsProvider, config );
        }

        @Override
        protected ConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan, Bootstrap bootstrap,
                                                       MetricsProvider metricsProvider, Config config, boolean ownsEventLoopGroup,
                                                       RoutingContext routingContext )
        {
            capturedSecurityPlans.add( securityPlan );
            return super.createConnectionPool( authToken, securityPlan, bootstrap, metricsProvider, config, ownsEventLoopGroup, routingContext );
        }
    }
}
