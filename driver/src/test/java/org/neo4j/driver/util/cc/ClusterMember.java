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
package org.neo4j.driver.util.cc;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Scanner;

import org.neo4j.driver.internal.BoltServerAddress;

import static java.util.Objects.requireNonNull;

public class ClusterMember
{
    public static final String SIMPLE_SCHEME = "bolt://";
    public static final String ROUTING_SCHEME = "neo4j://";

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

    public void dumpDebugLog() throws FileNotFoundException
    {
        Scanner input = new Scanner( new File( path.toAbsolutePath().toString() + "/logs/debug.log" ));

        while (input.hasNextLine())
        {
            System.out.println(input.nextLine());
        }
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
               "boltUri=" + boltUri +
               ", boltAddress=" + boltAddress +
               ", path=" + path +
               '}';
    }

    private static BoltServerAddress newBoltServerAddress( URI uri )
    {
        try
        {
            return new BoltServerAddress( uri ).resolve();
        }
        catch ( UnknownHostException e )
        {
            throw new RuntimeException( e );
        }
    }
}
