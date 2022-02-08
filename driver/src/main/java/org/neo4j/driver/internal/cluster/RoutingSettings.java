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
package org.neo4j.driver.internal.cluster;

import static java.util.concurrent.TimeUnit.SECONDS;

public class RoutingSettings
{
    public static final long STALE_ROUTING_TABLE_PURGE_DELAY_MS = SECONDS.toMillis( 30 );
    public static final RoutingSettings DEFAULT = new RoutingSettings( STALE_ROUTING_TABLE_PURGE_DELAY_MS );

    private final RoutingContext routingContext;
    private final long routingTablePurgeDelayMs;

    public RoutingSettings( long routingTablePurgeDelayMs )
    {
        this( routingTablePurgeDelayMs, RoutingContext.EMPTY );
    }

    public RoutingSettings( long routingTablePurgeDelayMs, RoutingContext routingContext )
    {
        this.routingContext = routingContext;
        this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
    }

    public RoutingSettings withRoutingContext( RoutingContext newRoutingContext )
    {
        return new RoutingSettings( routingTablePurgeDelayMs, newRoutingContext );
    }

    public RoutingContext routingContext()
    {
        return routingContext;
    }

    public long routingTablePurgeDelayMs()
    {
        return routingTablePurgeDelayMs;
    }
}
