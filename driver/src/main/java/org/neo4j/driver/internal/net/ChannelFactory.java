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
package org.neo4j.driver.internal.net;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;

import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.TLSSocketChannel;
import org.neo4j.driver.v1.Logger;

class ChannelFactory
{
    static ByteChannel create( BoltServerAddress address, SecurityPlan securityPlan, int timeoutMillis, Logger log )
            throws IOException
    {
        SocketChannel soChannel = SocketChannel.open();
        soChannel.setOption( StandardSocketOptions.SO_REUSEADDR, true );
        soChannel.setOption( StandardSocketOptions.SO_KEEPALIVE, true );
        connect( soChannel, address, timeoutMillis );

        ByteChannel channel = soChannel;

        if ( securityPlan.requiresEncryption() )
        {
            channel = TLSSocketChannel.create( address, securityPlan, soChannel, log );
        }

        if ( log.isTraceEnabled() )
        {
            channel = new LoggingByteChannel( channel, log );
        }

        return channel;
    }

    private static void connect( SocketChannel soChannel, BoltServerAddress address, int timeoutMillis )
            throws IOException
    {
        Socket socket = soChannel.socket();
        try
        {
            socket.connect( address.toSocketAddress(), timeoutMillis );
        }
        catch ( SocketTimeoutException e )
        {
            throw new ConnectException( "Timeout " + timeoutMillis + "ms expired" + e );
        }
    }
}
