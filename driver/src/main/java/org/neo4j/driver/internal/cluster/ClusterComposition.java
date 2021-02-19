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

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.BoltServerAddress;

public final class ClusterComposition
{
    private static final long MAX_TTL = Long.MAX_VALUE / 1000L;
    private static final Function<Value,BoltServerAddress> OF_BoltServerAddress = value -> new BoltServerAddress( value.asString() );

    private final Set<BoltServerAddress> readers;
    private final Set<BoltServerAddress> writers;
    private final Set<BoltServerAddress> routers;
    private final long expirationTimestamp;

    private ClusterComposition( long expirationTimestamp )
    {
        this.readers = new LinkedHashSet<>();
        this.writers = new LinkedHashSet<>();
        this.routers = new LinkedHashSet<>();
        this.expirationTimestamp = expirationTimestamp;
    }

    /** For testing */
    public ClusterComposition(
            long expirationTimestamp,
            Set<BoltServerAddress> readers,
            Set<BoltServerAddress> writers,
            Set<BoltServerAddress> routers )
    {
        this( expirationTimestamp );
        this.readers.addAll( readers );
        this.writers.addAll( writers );
        this.routers.addAll( routers );
    }

    public boolean hasWriters()
    {
        return !writers.isEmpty();
    }

    public boolean hasRoutersAndReaders()
    {
        return !routers.isEmpty() && !readers.isEmpty();
    }

    public Set<BoltServerAddress> readers()
    {
        return new LinkedHashSet<>( readers );
    }

    public Set<BoltServerAddress> writers()
    {
        return new LinkedHashSet<>( writers );
    }

    public Set<BoltServerAddress> routers()
    {
        return new LinkedHashSet<>( routers );
    }

    public long expirationTimestamp() {
        return this.expirationTimestamp;
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
        ClusterComposition that = (ClusterComposition) o;
        return expirationTimestamp == that.expirationTimestamp &&
               Objects.equals( readers, that.readers ) &&
               Objects.equals( writers, that.writers ) &&
               Objects.equals( routers, that.routers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( readers, writers, routers, expirationTimestamp );
    }

    @Override
    public String toString()
    {
        return "ClusterComposition{" +
               "readers=" + readers +
               ", writers=" + writers +
               ", routers=" + routers +
               ", expirationTimestamp=" + expirationTimestamp +
               '}';
    }

    public static ClusterComposition parse( Record record, long now )
    {
        if ( record == null )
        {
            return null;
        }

        final ClusterComposition result = new ClusterComposition( expirationTimestamp( now, record ) );
        record.get( "servers" ).asList( new Function<Value,Void>()
        {
            @Override
            public Void apply( Value value )
            {
                result.servers( value.get( "role" ).asString() )
                        .addAll( value.get( "addresses" ).asList( OF_BoltServerAddress ) );
                return null;
            }
        } );
        return result;
    }

    private static long expirationTimestamp( long now, Record record )
    {
        long ttl = record.get( "ttl" ).asLong();
        long expirationTimestamp = now + ttl * 1000;
        if ( ttl < 0 || ttl >= MAX_TTL || expirationTimestamp < 0 )
        {
            expirationTimestamp = Long.MAX_VALUE;
        }
        return expirationTimestamp;
    }

    private Set<BoltServerAddress> servers( String role )
    {
        switch ( role )
        {
        case "READ":
            return readers;
        case "WRITE":
            return writers;
        case "ROUTE":
            return routers;
        default:
            throw new IllegalArgumentException( "invalid server role: " + role );
        }
    }
}
