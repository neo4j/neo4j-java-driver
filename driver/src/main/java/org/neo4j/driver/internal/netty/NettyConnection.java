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
package org.neo4j.driver.internal.netty;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.ResetMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.netty.ChannelAttributes.responseHandlersHolder;

// todo: keep state flags to prohibit interaction with released connections
public class NettyConnection implements AsyncConnection
{
    private final Future<Channel> channelFuture;
    private final NettyChannelPool channelPool;

    private Channel channel;
    private ResponseHandlersHolder responseHandlersHolder;
    private final AtomicBoolean autoReadEnabled = new AtomicBoolean( true );

    private final NettyConnectionState state = new NettyConnectionState();

    public NettyConnection( Future<Channel> channelFuture, NettyChannelPool channelPool )
    {
        this.channelFuture = channelFuture;
        this.channelPool = channelPool;
    }

    @Override
    public boolean tryMarkInUse()
    {
        return state.markInUse();
    }

    @Override
    public void enableAutoRead()
    {
        if ( autoReadEnabled.compareAndSet( false, true ) )
        {
            System.out.println( "=== enableAutoRead" );
            setAutoRead( true );
        }
    }

    @Override
    public void disableAutoRead()
    {
        if ( autoReadEnabled.compareAndSet( true, false ) )
        {
            System.out.println( "=== disableAutoRead" );
            setAutoRead( false );
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, ResponseHandler handler )
    {
        write( new RunMessage( statement, parameters ), handler, false );
    }

    @Override
    public void pullAll( ResponseHandler handler )
    {
        write( PullAllMessage.PULL_ALL, handler, false );
    }

    @Override
    public void flush()
    {
        if ( tryExtractChannel() )
        {
            channel.eventLoop().execute( new Runnable()
            {
                @Override
                public void run()
                {
                    channel.flush();
                }
            } );
        }
        else
        {
            channelFuture.addListener( FlushListener.INSTANCE );
        }
    }

    @Override
    public <T> Promise<T> newPromise()
    {
        if ( tryExtractChannel() )
        {
            return channel.eventLoop().newPromise();
        }
        else
        {
            return GlobalEventExecutor.INSTANCE.newPromise();
        }
    }

    @Override
    public void release()
    {
        if ( state.release() )
        {
            write( ResetMessage.RESET, new ReleaseChannelHandler( channelFuture, channelPool ), true );
        }
    }

    @Override
    public Promise<Void> forceRelease()
    {
        Promise<Void> releasePromise = newPromise();

        if ( state.forceRelease() )
        {
            write( ResetMessage.RESET, new ReleaseChannelHandler( channelFuture, channelPool, releasePromise ), true );
        }
        else
        {
            releasePromise.setSuccess( null );
        }

        return releasePromise;
    }

    private void write( final Message message, final ResponseHandler handler, final boolean flush )
    {
        if ( tryExtractChannel() )
        {
            channel.eventLoop().execute( new Runnable()
            {
                @Override
                public void run()
                {
                    responseHandlersHolder.queue( handler );
                    if ( flush )
                    {
                        channel.writeAndFlush( message );
                    }
                    else
                    {
                        channel.write( message );
                    }
                }
            } );
        }
        else
        {
            channelFuture.addListener( new WriteListener( message, handler, flush ) );
        }
    }

    private void setAutoRead( boolean value )
    {
        if ( tryExtractChannel() )
        {
            channel.config().setAutoRead( value );
        }
        else
        {
            channelFuture.addListener( AutoReadListener.forValue( value ) );
        }
    }

    private boolean tryExtractChannel()
    {
        if ( channel != null )
        {
            return true;
        }
        else if ( channelFuture.isSuccess() )
        {
            channel = requireNonNull( channelFuture.getNow() );
            responseHandlersHolder = requireNonNull( responseHandlersHolder( channel ) );
            return true;
        }
        else
        {
            return false;
        }
    }
}
