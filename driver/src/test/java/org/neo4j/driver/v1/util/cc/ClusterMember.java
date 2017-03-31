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
package org.neo4j.driver.v1.util.cc;

import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static java.util.Objects.requireNonNull;

public class ClusterMember
{
    private static final String SIMPLE_SCHEME = "bolt://";
    private static final String ROUTING_SCHEME = "bolt+routing://";

    private final URI boltUri;
    private final BoltServerAddress boltAddress;
    private final Path path;

    public ClusterMember( URI boltUri, Path path )
    {
        this.boltUri = requireNonNull( boltUri );
        this.boltAddress = newBoltServerAddress( boltUri );
        this.path = requireNonNull( path );
    }

    public URI getBoltUri()
    {
        return boltUri;
    }

    public URI getRoutingUri()
    {
        return URI.create( boltUri.toString().replace( SIMPLE_SCHEME, ROUTING_SCHEME ) );
    }

    public BoltServerAddress getBoltAddress()
    {
        return boltAddress;
    }

    public Path getPath()
    {
        return path;
    }

    @Override
    public String toString()
    {
        return "ClusterMember{" +
               "boltUri=" + boltUri +
               ", path=" + path +
               "}";
    }

    private static BoltServerAddress newBoltServerAddress( URI uri )
    {
        try
        {
            return BoltServerAddress.from( uri ).resolve();
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( e );
        }
    }
}
