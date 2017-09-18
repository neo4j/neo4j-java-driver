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
package org.neo4j.driver.internal.async;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.ProtocolUtil.handshake;

public class ChannelConnectedListener implements ChannelFutureListener
{
    private final BoltServerAddress address;
    private final ChannelPromise handshakeCompletedPromise;
    private final Logging logging;

    public ChannelConnectedListener( BoltServerAddress address, ChannelPromise handshakeCompletedPromise,
            Logging logging )
    {
        this.address = requireNonNull( address );
        this.handshakeCompletedPromise = requireNonNull( handshakeCompletedPromise );
        this.logging = requireNonNull( logging );
    }

    @Override
    public void operationComplete( ChannelFuture future )
    {
        Channel channel = future.channel();

        if ( future.isSuccess() )
        {
            channel.pipeline().addLast( new HandshakeResponseHandler( handshakeCompletedPromise, logging ) );
            ChannelFuture handshakeFuture = channel.writeAndFlush( handshake() );

            handshakeFuture.addListener( new ChannelFutureListener()
            {
                @Override
                public void operationComplete( ChannelFuture future ) throws Exception
                {
                    if ( !future.isSuccess() )
                    {
                        handshakeCompletedPromise.setFailure( future.cause() );
                    }
                }
            } );
        }
        else
        {
            handshakeCompletedPromise.setFailure( databaseUnavailableError( address, future.cause() ) );
        }
    }

    private static Throwable databaseUnavailableError( BoltServerAddress address, Throwable cause )
    {
        return new ServiceUnavailableException( format(
                "Unable to connect to %s, ensure the database is running and that there " +
                "is a working network connection to it.", address ), cause );
    }
}
