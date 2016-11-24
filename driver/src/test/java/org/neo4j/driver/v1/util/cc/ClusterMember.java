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
package org.neo4j.driver.v1.util.cc;

import java.net.URI;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

public class ClusterMember
{
    private final URI boltUri;
    private final URI httpUri;
    private final Path path;
    private final ClusterMemberRole role;

    public ClusterMember( URI boltUri, URI httpUri, Path path )
    {
        this( boltUri, httpUri, path, ClusterMemberRole.UNKNOWN );
    }

    public ClusterMember( URI boltUri, URI httpUri, Path path, ClusterMemberRole role )
    {
        this.boltUri = requireNonNull( boltUri );
        this.httpUri = requireNonNull( httpUri );
        this.path = requireNonNull( path );
        this.role = requireNonNull( role );
    }

    public ClusterMember withRole( ClusterMemberRole role )
    {
        return new ClusterMember( boltUri, httpUri, path, role );
    }

    public URI getBoltUri()
    {
        return boltUri;
    }

    public URI getRoutingUri()
    {
        return URI.create( boltUri.toString().replace( "bolt://", "bolt+routing://" ) );
    }

    public URI getHttpUri()
    {
        return httpUri;
    }

    public Path getPath()
    {
        return path;
    }

    public ClusterMemberRole getRole()
    {
        return role;
    }

    @Override
    public String toString()
    {
        return "ClusterMember{" +
               "boltUri=" + boltUri +
               ", httpUri=" + httpUri +
               ", path=" + path +
               ", role=" + role +
               "}";
    }
}
