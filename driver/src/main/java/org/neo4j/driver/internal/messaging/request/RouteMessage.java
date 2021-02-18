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
package org.neo4j.driver.internal.messaging.request;

import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.messaging.Message;

import static java.util.Collections.unmodifiableMap;

/**
 * From the application point of view it is not interesting to know about the role a member plays in the cluster. Instead, the application needs to know which
 * instance can provide the wanted service.
 * <p>
 * This message is used to fetch this routing information.
 */
public class RouteMessage implements Message
{
    public final static byte SIGNATURE = 0x66;
    private final Map<String,Value> routingContext;
    private final String databaseName;

    /**
     * Constructor
     *
     * @param routingContext The routing context used to define the routing table. Multi-datacenter deployments is one of its use cases.
     * @param databaseName   The name of the database to get the routing table for.
     */
    public RouteMessage( Map<String,Value> routingContext, String databaseName )
    {
        this.routingContext = unmodifiableMap( routingContext );
        this.databaseName = databaseName;
    }

    public Map<String,Value> getRoutingContext()
    {
        return routingContext;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }

    @Override
    public String toString()
    {
        return String.format( "ROUTE %s %s", routingContext, databaseName );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        RouteMessage that = (RouteMessage) o;
        return routingContext.equals( that.routingContext ) &&
               Objects.equals( databaseName, that.databaseName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( routingContext, databaseName );
    }
}
