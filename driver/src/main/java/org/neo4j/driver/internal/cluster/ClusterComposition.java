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

import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Function;

final class ClusterComposition
{
    private static final long MAX_TTL = Long.MAX_VALUE / 1000L;
    private static final Function<Value,BoltServerAddress> OF_BoltServerAddress =
            new Function<Value,BoltServerAddress>()
            {
                @Override
                public BoltServerAddress apply( Value value )
                {
                    return new BoltServerAddress( value.asString() );
                }
            };

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
    ClusterComposition(
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
    public String toString()
    {
        return "ClusterComposition{" +
                "expirationTimestamp=" + expirationTimestamp +
                ", readers=" + readers +
                ", writers=" + writers +
                ", routers=" + routers +
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
