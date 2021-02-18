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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.junit.jupiter.api.Assertions.fail;

@ParallelizableIT
class SharedEventLoopIT
{
    private final DriverFactory driverFactory = new DriverFactory();

    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    @Test
    void testDriverShouldNotCloseSharedEventLoop()
    {
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup( 1 );

        try
        {
            Driver driver1 = createDriver( eventLoopGroup );
            Driver driver2 = createDriver( eventLoopGroup );

            testConnection( driver1 );
            testConnection( driver2 );

            driver1.close();

            testConnection( driver2 );
            driver2.close();
        }
        finally
        {
            eventLoopGroup.shutdownGracefully( 100, 100, TimeUnit.MILLISECONDS );
        }
    }

    @Test
    void testDriverShouldUseSharedEventLoop()
    {
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup( 1 );

        Driver driver = createDriver( eventLoopGroup );
        testConnection( driver );

        eventLoopGroup.shutdownGracefully( 100, 100, TimeUnit.MILLISECONDS );

        // the driver should fail if it really uses the provided event loop
        // if the call succeeds, it meas that the driver created its own event loop
        try
        {
            testConnection( driver );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            // ignored
        }
    }

    private Driver createDriver( EventLoopGroup eventLoopGroup )
    {
        return driverFactory.newInstance( neo4j.uri(), neo4j.authToken(), RoutingSettings.DEFAULT, RetrySettings.DEFAULT, Config.defaultConfig(),
                                          eventLoopGroup, SecurityPlanImpl.insecure() );
    }

    private void testConnection( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            session.run( "RETURN 1" );
        }
    }
}
