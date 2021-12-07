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
package org.neo4j.driver.stress;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.integration.NestedQueries;
import org.neo4j.driver.util.cc.ClusterExtension;

import static org.neo4j.driver.Logging.none;
import static org.neo4j.driver.SessionConfig.builder;

public class CausalClusteringIT implements NestedQueries
{
    @RegisterExtension
    static final ClusterExtension clusterRule = new ClusterExtension();

    private Driver driver;

    @Override
    public Session newSession( AccessMode mode )
    {
        if ( driver == null )
        {
            driver = createDriver( clusterRule.getCluster().getRoutingUri() );
        }

        return driver.session( builder().withDefaultAccessMode( mode ).build() );
    }

    @AfterEach
    void tearDown()
    {
        if ( driver != null )
        {
            driver.close();
        }
    }

    private Driver createDriver( URI boltUri )
    {
        return createDriver( boltUri, configWithoutLogging() );
    }

    private Driver createDriver( URI boltUri, Config config )
    {
        return GraphDatabase.driver( boltUri, clusterRule.getDefaultAuthToken(), config );
    }

    private static Config configWithoutLogging()
    {
        return Config.builder().withLogging( none() ).build();
    }
}
