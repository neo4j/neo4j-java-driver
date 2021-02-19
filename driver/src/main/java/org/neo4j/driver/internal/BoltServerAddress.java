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
package org.neo4j.driver.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.neo4j.driver.net.ServerAddress;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Holds a host and port pair that denotes a Bolt server address.
 */
public class BoltServerAddress implements ServerAddress
{
    public static final int DEFAULT_PORT = 7687;
    public static final BoltServerAddress LOCAL_DEFAULT = new BoltServerAddress( "localhost", DEFAULT_PORT );

    private final String host; // This could either be the same as originalHost or it is an IP address resolved from the original host.
    private final int port;
    private final String stringValue;

    private InetAddress resolved;

    public BoltServerAddress( String address )
    {
        this( uriFrom( address ) );
    }

    public BoltServerAddress( URI uri )
    {
        this( hostFrom( uri ), portFrom( uri ) );
    }

    public BoltServerAddress( String host, int port )
    {
        this( host, null, port );
    }

    private BoltServerAddress( String host, InetAddress resolved, int port )
    {
        this.host = requireNonNull( host, "host" );
        this.resolved = resolved;
        this.port = requireValidPort( port );
        this.stringValue = resolved != null ? String.format( "%s(%s):%d", host, resolved.getHostAddress(), port ) : String.format( "%s:%d", host, port );
    }

    public static BoltServerAddress from( ServerAddress address )
    {
        return address instanceof BoltServerAddress
               ? (BoltServerAddress) address
               : new BoltServerAddress( address.host(), address.port() );
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
        BoltServerAddress that = (BoltServerAddress) o;
        return port == that.port && host.equals( that.host );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( host, port );
    }

    @Override
    public String toString()
    {
        return stringValue;
    }

    /**
     * Create a {@link SocketAddress} from this bolt address. This method always attempts to resolve the hostname into
     * an {@link InetAddress}.
     *
     * @return new socket address.
     * @see InetSocketAddress
     */
    public SocketAddress toSocketAddress()
    {
        return resolved == null ? new InetSocketAddress( host, port ) : new InetSocketAddress( resolved, port );
    }

    /**
     * Resolve the host name down to an IP address
     *
     * @return a new address instance
     * @throws UnknownHostException if no IP address for the host could be found
     * @see InetAddress#getByName(String)
     */
    public BoltServerAddress resolve() throws UnknownHostException
    {
        return new BoltServerAddress( host, InetAddress.getByName( host ), port );
    }

    /**
     * Resolve the host name down to all IP addresses that can be resolved to
     *
     * @return an array of new address instances that holds resolved addresses
     * @throws UnknownHostException if no IP address for the host could be found
     * @see InetAddress#getAllByName(String)
     */
    public List<BoltServerAddress> resolveAll() throws UnknownHostException
    {
        return Stream.of( InetAddress.getAllByName( host ) )
                .map( address -> new BoltServerAddress( host, address, port ) )
                .collect( toList() );
    }

    @Override
    public String host()
    {
        return host;
    }

    @Override
    public int port()
    {
        return port;
    }

    public boolean isResolved()
    {
        return resolved != null;
    }

    private static String hostFrom( URI uri )
    {
        String host = uri.getHost();
        if ( host == null )
        {
            throw invalidAddressFormat( uri );
        }
        return host;
    }

    private static int portFrom( URI uri )
    {
        int port = uri.getPort();
        return port == -1 ? DEFAULT_PORT : port;
    }

    private static URI uriFrom( String address )
    {
        String scheme;
        String hostPort;

        String[] schemeSplit = address.split( "://" );
        if ( schemeSplit.length == 1 )
        {
            // URI can't parse addresses without scheme, prepend fake "bolt://" to reuse the parsing facility
            scheme = "bolt://";
            hostPort = hostPortFrom( schemeSplit[0] );
        }
        else if ( schemeSplit.length == 2 )
        {
            scheme = schemeSplit[0] + "://";
            hostPort = hostPortFrom( schemeSplit[1] );
        }
        else
        {
            throw invalidAddressFormat( address );
        }

        return URI.create( scheme + hostPort );
    }

    private static String hostPortFrom( String address )
    {
        if ( address.startsWith( "[" ) )
        {
            // expected to be an IPv6 address like [::1] or [::1]:7687
            return address;
        }

        boolean containsSingleColon = address.indexOf( ":" ) == address.lastIndexOf( ":" );
        if ( containsSingleColon )
        {
            // expected to be an IPv4 address with or without port like 127.0.0.1 or 127.0.0.1:7687
            return address;
        }

        // address contains multiple colons and does not start with '['
        // expected to be an IPv6 address without brackets
        return "[" + address + "]";
    }

    private static RuntimeException invalidAddressFormat( URI uri )
    {
        return invalidAddressFormat( uri.toString() );
    }

    private static RuntimeException invalidAddressFormat( String address )
    {
        return new IllegalArgumentException( "Invalid address format `" + address + "`" );
    }

    private static int requireValidPort( int port )
    {
        if ( port >= 0 && port <= 65_535 )
        {
            return port;
        }
        throw new IllegalArgumentException( "Illegal port: " + port );
    }
}
