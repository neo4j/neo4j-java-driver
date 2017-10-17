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
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Promise;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.handlers.ResetResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PullAllMessage;
import org.neo4j.driver.internal.messaging.ResetMessage;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Value;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.util.Futures.asCompletionStage;

// todo: keep state flags to prohibit interaction with released connections
public class NettyConnection implements Connection
{
    private final Channel channel;
    private final InboundMessageDispatcher messageDispatcher;
    private final ChannelPool channelPool;
    private final Clock clock;

    private final AtomicBoolean autoReadEnabled = new AtomicBoolean( true );

    private final NettyConnectionState state = new NettyConnectionState();

    public NettyConnection( Channel channel, ChannelPool channelPool, Clock clock )
    {
        this.channel = channel;
        this.messageDispatcher = messageDispatcher( channel );
        this.channelPool = channelPool;
        this.clock = clock;
    }

    @Override
    public boolean isInUse()
    {
        return state.isInUse();
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
            setAutoRead( true );
        }
    }

    @Override
    public void disableAutoRead()
    {
        if ( autoReadEnabled.compareAndSet( true, false ) )
        {
            setAutoRead( false );
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, ResponseHandler runHandler,
            ResponseHandler pullAllHandler )
    {
        run( statement, parameters, runHandler, pullAllHandler, false );
    }

    @Override
    public void runAndFlush( String statement, Map<String,Value> parameters, ResponseHandler runHandler,
            ResponseHandler pullAllHandler )
    {
        run( statement, parameters, runHandler, pullAllHandler, true );
    }

    @Override
    public void releaseInBackground()
    {
        if ( state.release() )
        {
            reset( new ResetResponseHandler( channel, channelPool, messageDispatcher, clock ) );
        }
    }

    @Override
    public CompletionStage<Void> releaseNow()
    {
        if ( state.forceRelease() )
        {
            Promise<Void> releasePromise = channel.eventLoop().newPromise();
            reset( new ResetResponseHandler( channel, channelPool, messageDispatcher, clock, releasePromise ) );
            return asCompletionStage( releasePromise );
        }
        else
        {
            return completedFuture( null );
        }
    }

    @Override
    public BoltServerAddress serverAddress()
    {
        return ChannelAttributes.serverAddress( channel );
    }

    @Override
    public ServerVersion serverVersion()
    {
        return ChannelAttributes.serverVersion( channel );
    }

    private void run( String statement, Map<String,Value> parameters, ResponseHandler runHandler,
            ResponseHandler pullAllHandler, boolean flush )
    {
        writeMessagesInEventLoop( new RunMessage( statement, parameters ), runHandler, PullAllMessage.PULL_ALL,
                pullAllHandler, flush );
    }

    private void reset( ResponseHandler resetHandler )
    {
        channel.eventLoop().execute( () ->
        {
            messageDispatcher.muteAckFailure();
            writeAndFlushMessage( ResetMessage.RESET, resetHandler );
        } );
    }

    private void writeMessagesInEventLoop( Message message1, ResponseHandler handler1, Message message2,
            ResponseHandler handler2, boolean flush )
    {
        channel.eventLoop().execute( () -> writeMessages( message1, handler1, message2, handler2, flush ) );
    }

    private void writeMessages( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2,
            boolean flush )
    {
        messageDispatcher.queue( handler1 );
        messageDispatcher.queue( handler2 );

        channel.write( message1, channel.voidPromise() );

        if ( flush )
        {
            channel.writeAndFlush( message2, channel.voidPromise() );
        }
        else
        {
            channel.write( message2, channel.voidPromise() );
        }
    }

    private void writeAndFlushMessage( Message message, ResponseHandler handler )
    {
        messageDispatcher.queue( handler );
        channel.writeAndFlush( message, channel.voidPromise() );
    }

    private void setAutoRead( boolean value )
    {
        channel.config().setAutoRead( value );
    }
}
