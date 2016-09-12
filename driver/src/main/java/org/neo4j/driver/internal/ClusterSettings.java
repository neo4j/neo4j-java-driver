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

package org.neo4j.driver.internal;

import org.neo4j.driver.v1.Config;

public class ClusterSettings
{
    private final int readRetry;
    private final int minimumNumberOfServers;

    public ClusterSettings( int readRetry, int minimumNumberOfServers )
    {
        this.readRetry = readRetry;
        this.minimumNumberOfServers = minimumNumberOfServers;
    }

    public static ClusterSettings fromConfig( Config config )
    {
        return new ClusterSettings( config.maximumReadRetriesForCluster(), config.minimumKnownClusterSize() ) ;
    }

    public int readRetry()
    {
        return readRetry;
    }

    public int minimumNumberOfServers()
    {
        return minimumNumberOfServers;
    }
}
