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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CodecException;

import java.io.IOException;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;

public class ChannelErrorHandler extends ChannelInboundHandlerAdapter
{
    private final Logger log;

    private InboundMessageDispatcher messageDispatcher;
    private boolean failed;

    public ChannelErrorHandler( Logging logging )
    {
        this.log = logging.getLog( getClass().getSimpleName() );
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx )
    {
        messageDispatcher = requireNonNull( messageDispatcher( ctx.channel() ) );
    }

    @Override
    public void handlerRemoved( ChannelHandlerContext ctx )
    {
        messageDispatcher = null;
        failed = false;
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx )
    {
        log.debug( "Channel inactive: %s", ctx.channel() );

        if ( !failed )
        {
            // channel became inactive not because of a fatal exception that came from exceptionCaught
            // it is most likely inactive because actual network connection broke
            ServiceUnavailableException error = new ServiceUnavailableException(
                    "Connection to the database terminated. " +
                    "This can happen due to network instabilities, or due to restarts of the database" );

            fail( ctx, error );
        }
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable error )
    {
        if ( failed )
        {
            log.warn( "Another fatal error in the pipeline of " + ctx.channel(), error );
        }
        else
        {
            failed = true;
            log.error( "Fatal error in the pipeline of " + ctx.channel(), error );
            fail( ctx, error );
        }
    }

    private void fail( ChannelHandlerContext ctx, Throwable error )
    {
        Throwable cause = transformError( error );
        messageDispatcher.handleFatalError( cause );
        log.debug( "Closing channel because of an error: %s", ctx.channel() );
        ctx.close();
    }

    private Throwable transformError( Throwable error )
    {
        if ( error instanceof CodecException )
        {
            // unwrap exception from message encoder/decoder
            error = error.getCause();
        }

        if ( error instanceof IOException )
        {
            return new ServiceUnavailableException( "Connection to the database failed", error );
        }
        else
        {
            return error;
        }
    }
}
