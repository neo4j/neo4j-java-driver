/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.util;

import io.netty.bootstrap.Bootstrap;

import java.net.URI;

import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.async.BootstrapFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;

public class DriverFactoryWithOneEventLoopThread extends DriverFactory
{
    public Driver newInstance( URI uri, AuthToken authToken, Config config )
    {
        return newInstance( uri, authToken, new RoutingSettings( 1, 0 ), RetrySettings.DEFAULT, config );
    }

    @Override
    protected Bootstrap createBootstrap()
    {
        return BootstrapFactory.newBootstrap( 1 );
    }
}
