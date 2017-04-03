/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.cluster;

public class RoutingSettings
{
    private final int maxRoutingFailures;
    private final long retryTimeoutDelay;
    private final RoutingContext routingContext;

    public RoutingSettings( int maxRoutingFailures, long retryTimeoutDelay )
    {
        this( maxRoutingFailures, retryTimeoutDelay, RoutingContext.EMPTY );
    }

    public RoutingSettings( int maxRoutingFailures, long retryTimeoutDelay, RoutingContext routingContext )
    {
        this.maxRoutingFailures = maxRoutingFailures;
        this.retryTimeoutDelay = retryTimeoutDelay;
        this.routingContext = routingContext;
    }

    public RoutingSettings withRoutingContext( RoutingContext newRoutingContext )
    {
        return new RoutingSettings( maxRoutingFailures, retryTimeoutDelay, newRoutingContext );
    }

    public int maxRoutingFailures()
    {
        return maxRoutingFailures;
    }

    public long retryTimeoutDelay()
    {
        return retryTimeoutDelay;
    }

    public RoutingContext routingContext()
    {
        return routingContext;
    }
}
