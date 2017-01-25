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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ProtocolException;
import org.neo4j.driver.v1.exceptions.value.ValueException;
import org.neo4j.driver.v1.util.Function;

import static java.lang.String.format;
import static org.neo4j.driver.internal.cluster.ClusterComposition.Provider.PROTOCOL_ERROR_MESSAGE;

final class ClusterComposition
{
    interface Provider
    {
        String GET_SERVERS = "dbms.cluster.routing.getServers";
        String CALL_GET_SERVERS = "CALL " + GET_SERVERS;
        String PROTOCOL_ERROR_MESSAGE = "Failed to parse '" + GET_SERVERS + "' result received from server.";

        ClusterComposition getClusterComposition( Connection connection )
                throws ProtocolException, ProcedureNotFoundException;

        class ProcedureNotFoundException extends Exception
        {
            ProcedureNotFoundException( String message )
            {
                super( message );
            }

            ProcedureNotFoundException( String message, Throwable e )
            {
                super( message, e );
            }
        }

        final class Default implements Provider
        {
            private static final Statement CALL_GET_SERVER = new Statement( Provider.CALL_GET_SERVERS );
            private final Clock clock;
            private final Logger log;

            Default( Clock clock, Logger log )
            {
                this.clock = clock;
                this.log = log;
            }

            @Override
            public ClusterComposition getClusterComposition( Connection connection )
                    throws ProtocolException, ProcedureNotFoundException
            {
                List<Record> records = getServers( connection );
                log.info( "Got getServers response: %s", records );
                long now = clock.millis();

                if ( records.size() != 1 )
                {
                    throw new ProtocolException( format(
                            "%s%nRecords received '%s' is too few or too many.", PROTOCOL_ERROR_MESSAGE,
                            records.size() ) );
                }
                ClusterComposition cluster = read( records.get( 0 ), now );
                if ( cluster.isIllegalResponse() )
                {
                    throw new ProtocolException( format( "%s%nNo router or reader found in response.",
                            PROTOCOL_ERROR_MESSAGE ) );
                }
                return cluster;
            }

            private List<Record> getServers( Connection connection ) throws ProcedureNotFoundException
            {
                try
                {
                    return NetworkSession.run( connection, CALL_GET_SERVER ).list();
                }
                catch ( ClientException e )
                {
                    throw new ProcedureNotFoundException( format("Failed to call '%s' procedure on server. " +
                                   "Please make sure that there is a Neo4j 3.1+ causal cluster up running.",
                                    GET_SERVERS ), e );
                }
            }
        }
    }

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
    private final Set<BoltServerAddress> readers, writers, routers;
    final long expirationTimestamp;

    private ClusterComposition( long expirationTimestamp )
    {
        this.readers = new HashSet<>();
        this.writers = new HashSet<>();
        this.routers = new HashSet<>();
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

    public boolean isValid()
    {
        return !writers.isEmpty();
    }
    public boolean isIllegalResponse()
    {
        return routers.isEmpty() || readers.isEmpty();
    }

    public Set<BoltServerAddress> readers()
    {
        return new HashSet<>( readers );
    }

    public Set<BoltServerAddress> writers()
    {
        return new HashSet<>( writers );
    }

    public Set<BoltServerAddress> routers()
    {
        return new HashSet<>( routers );
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

    private static ClusterComposition read( Record record, long now )
    {
        if ( record == null )
        {
            return null;
        }
        try
        {
            final ClusterComposition result;
            result = new ClusterComposition( expirationTimestamp( now, record ) );
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
        catch ( ValueException e )
        {
            throw new ProtocolException( format( "%s%nUnparsable record received.", PROTOCOL_ERROR_MESSAGE ), e );
        }
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
