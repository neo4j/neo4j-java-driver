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

package org.neo4j.driver.internal.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;

import static java.lang.String.format;

/**
 * Holds a host and port pair that denotes a Bolt server address.
 */
public class BoltServerAddress
{
    public static final int DEFAULT_PORT = 7687;
    public static final BoltServerAddress LOCAL_DEFAULT = new BoltServerAddress( "localhost", DEFAULT_PORT );

    public static BoltServerAddress from( URI uri )
    {
        int port = uri.getPort();
        if ( port == -1 )
        {
            port = DEFAULT_PORT;
        }
        return new BoltServerAddress( uri.getHost(), port );
    }

    private final String host;
    private final int port;

    private SocketAddress socketAddress = null;  // created lazily if required

    public BoltServerAddress( String host, int port )
    {
        this.host = host;
        this.port = port;
    }

    public BoltServerAddress( String host )
    {
        int colon = host.indexOf( ':' );
        if ( colon >= 0 )
        {
            this.port = Integer.parseInt( host.substring( colon + 1 ) );
            this.host = host.substring( 0, colon );
        }
        else
        {
            this.host = host;
            this.port = DEFAULT_PORT;
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( !(obj instanceof BoltServerAddress) )
        {
            return false;
        }
        BoltServerAddress address = (BoltServerAddress) obj;
        return host.equals( address.host ) && port == address.port;
    }

    @Override
    public int hashCode()
    {
        return 31 * host.hashCode() + port;
    }

    @Override
    public String toString()
    {
        return format( "%s:%d", host, port );
    }

    public SocketAddress toSocketAddress()
    {
        if (socketAddress == null)
        {
            socketAddress = new InetSocketAddress( host, port );
        }
        return socketAddress;
    }

    /**
     * Resolve the host name down to an IP address, if not already resolved.
     *
     * @return this instance if already resolved, otherwise a new address instance
     * @throws UnknownHostException
     */
    public BoltServerAddress resolve() throws UnknownHostException
    {
        String hostAddress = InetAddress.getByName( host ).getHostAddress();
        if ( hostAddress.equals( host ) )
        {
            return this;
        }
        else
        {
            return new BoltServerAddress( hostAddress, port );
        }
    }

    public String host()
    {
        return host;
    }

    public int port()
    {
        return port;
    }

    /**
     * Determine whether or not this address refers to the local machine. This
     * will generally be true for "localhost" or "127.x.x.x".
     *
     * @return true if local, false otherwise
     */
    public boolean isLocal()
    {
        try
        {
            // confirmed to work as desired with both "localhost" and "127.x.x.x"
            return InetAddress.getByName( host ).isLoopbackAddress();
        }
        catch ( UnknownHostException e )
        {
            // if it's unknown, it's not local so we can safely return false
            return false;
        }
    }
}
