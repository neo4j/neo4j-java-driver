/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.util.cc;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.Objects;

import org.neo4j.driver.internal.BoltServerAddress;

import static java.util.Objects.requireNonNull;

public class ClusterMember
{
    public static final String BOLT_SCHEMA = "bolt";
    private static final String NEO4J_SCHEMA = "neo4j";

    private final String hostPort;
    private final BoltServerAddress boltAddress;
    private final String name;

    public ClusterMember( String name, String hostPort )
    {
        this.name = name;
        this.hostPort = requireNonNull( hostPort );
        this.boltAddress = newBoltServerAddress( hostPort );
    }

    public URI getBoltUri()
    {
        return URI.create( String.format( "%s://%s", BOLT_SCHEMA, hostPort ) );
    }

    public URI getRoutingUri()
    {
        return URI.create( String.format( "%s://%s", NEO4J_SCHEMA, hostPort ) );
    }

    public BoltServerAddress getBoltAddress()
    {
        return boltAddress;
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
        ClusterMember that = (ClusterMember) o;
        return Objects.equals( boltAddress, that.boltAddress );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( boltAddress );
    }

    @Override
    public String toString()
    {
        return "ClusterMember{" +
               "name=" + name +
               ", hostPort=" + hostPort +
               ", boltAddress=" + boltAddress +
               '}';
    }

    private static BoltServerAddress newBoltServerAddress( String hostPort )
    {
        try
        {
            return new BoltServerAddress( hostPort ).resolve();
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( e );
        }
    }

    public String getName()
    {
        return this.name;
    }
}
