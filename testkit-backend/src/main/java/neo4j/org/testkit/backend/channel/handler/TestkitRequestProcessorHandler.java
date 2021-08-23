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
package neo4j.org.testkit.backend.channel.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.TestkitRequest;
import neo4j.org.testkit.backend.messages.responses.BackendError;
import neo4j.org.testkit.backend.messages.responses.DriverError;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.internal.async.pool.ConnectionPoolImpl;

public class TestkitRequestProcessorHandler extends ChannelInboundHandlerAdapter
{
    private final TestkitState testkitState = new TestkitState( this::writeAndFlush );
    private Channel channel;

    @Override
    public void channelRegistered( ChannelHandlerContext ctx ) throws Exception
    {
        channel = ctx.channel();
        super.channelRegistered( ctx );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg )
    {
        TestkitRequest testkitRequest = (TestkitRequest) msg;
        // Processing is done in a separate thread to avoid blocking EventLoop because some testing logic, like resolvers support, is blocking.
        CompletableFuture.runAsync(
                () ->
                {
                    try
                    {
                        testkitRequest.processAsync( testkitState )
                                      .thenAccept( responseOpt -> responseOpt.ifPresent( ctx::writeAndFlush ) )
                                      .exceptionally( throwable ->
                                                      {
                                                          ctx.writeAndFlush( createErrorResponse( throwable ) );
                                                          return null;
                                                      } );
                    }
                    catch ( Throwable throwable )
                    {
                        ctx.writeAndFlush( createErrorResponse( throwable ) );
                    }
                } );
    }

    private TestkitResponse createErrorResponse( Throwable throwable )
    {
        if ( throwable instanceof CompletionException )
        {
            throwable = throwable.getCause();
        }
        if ( throwable instanceof Neo4jException )
        {
            String id = testkitState.newId();
            Neo4jException e = (Neo4jException) throwable;
            testkitState.getErrors().put( id, e );
            return DriverError.builder()
                              .data( DriverError.DriverErrorBody.builder()
                                                                .id( id )
                                                                .errorType( e.getClass().getName() )
                                                                .code( e.code() )
                                                                .msg( e.getMessage() )
                                                                .build() )
                              .build();
        }
        else if ( isConnectionPoolClosedException( throwable ) || throwable instanceof UntrustedServerException )
        {
            String id = testkitState.newId();
            return DriverError.builder()
                              .data(
                                      DriverError.DriverErrorBody.builder()
                                                                 .id( id )
                                                                 .errorType( throwable.getClass().getName() )
                                                                 .msg( throwable.getMessage() )
                                                                 .build()
                              )
                              .build();
        }
        else
        {
            return BackendError.builder().data( BackendError.BackendErrorBody.builder().msg( throwable.toString() ).build() ).build();
        }
    }

    private boolean isConnectionPoolClosedException( Throwable throwable )
    {
        return throwable instanceof IllegalStateException && throwable.getMessage() != null &&
               throwable.getMessage().equals( ConnectionPoolImpl.CONNECTION_POOL_CLOSED_ERROR_MESSAGE );
    }

    private void writeAndFlush( TestkitResponse response )
    {
        if ( channel == null )
        {
            throw new IllegalStateException( "Called before channel is initialized" );
        }
        channel.writeAndFlush( response );
    }
}
