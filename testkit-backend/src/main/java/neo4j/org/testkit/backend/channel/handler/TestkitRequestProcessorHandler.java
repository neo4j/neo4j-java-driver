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
import neo4j.org.testkit.backend.CustomDriverError;
import neo4j.org.testkit.backend.FrontendError;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.TestkitRequest;
import neo4j.org.testkit.backend.messages.responses.BackendError;
import neo4j.org.testkit.backend.messages.responses.DriverError;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.internal.spi.ConnectionPool;

public class TestkitRequestProcessorHandler extends ChannelInboundHandlerAdapter
{
    private final TestkitState testkitState = new TestkitState( this::writeAndFlush );
    private final BiFunction<TestkitRequest,TestkitState,CompletionStage<TestkitResponse>> processorImpl;
    // Some requests require multiple threads
    private final Executor requestExecutorService = Executors.newFixedThreadPool( 10 );
    private Channel channel;

    public TestkitRequestProcessorHandler( BackendMode backendMode )
    {
        switch ( backendMode )
        {
        case ASYNC:
            processorImpl = TestkitRequest::processAsync;
            break;
        case REACTIVE:
            processorImpl = ( request, state ) -> request.processRx( state ).toFuture();
            break;
        default:
            processorImpl = TestkitRequestProcessorHandler::wrapSyncRequest;
            break;
        }
    }

    @Override
    public void channelRegistered( ChannelHandlerContext ctx ) throws Exception
    {
        channel = ctx.channel();
        super.channelRegistered( ctx );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg )
    {
        // Processing is done in a separate thread to avoid blocking EventLoop because some testing logic, like resolvers support, is blocking.
        requestExecutorService.execute( () ->
                                        {
                                            try
                                            {
                                                TestkitRequest request = (TestkitRequest) msg;
                                                CompletionStage<TestkitResponse> responseStage = processorImpl.apply( request, testkitState );
                                                responseStage.whenComplete( ( response, throwable ) ->
                                                                            {
                                                                                if ( throwable != null )
                                                                                {
                                                                                    ctx.writeAndFlush( createErrorResponse( throwable ) );
                                                                                }
                                                                                else if ( response != null )
                                                                                {
                                                                                    ctx.writeAndFlush( response );
                                                                                }
                                                                            } );
                                            }
                                            catch ( Throwable throwable )
                                            {
                                                ctx.writeAndFlush( createErrorResponse( throwable ) );
                                            }
                                        } );
    }

    private static CompletionStage<TestkitResponse> wrapSyncRequest( TestkitRequest testkitRequest, TestkitState testkitState )
    {
        CompletableFuture<TestkitResponse> result = new CompletableFuture<>();
        try
        {
            result.complete( testkitRequest.process( testkitState ) );
        }
        catch ( Throwable t )
        {
            result.completeExceptionally( t );
        }
        return result;
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
        else if ( throwable instanceof CustomDriverError )
        {
            throwable = throwable.getCause();
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
        else if ( throwable instanceof FrontendError )
        {
            return neo4j.org.testkit.backend.messages.responses.FrontendError.builder().build();
        }
        else
        {
            return BackendError.builder().data( BackendError.BackendErrorBody.builder().msg( throwable.toString() ).build() ).build();
        }
    }

    private boolean isConnectionPoolClosedException( Throwable throwable )
    {
        return throwable instanceof IllegalStateException && throwable.getMessage() != null &&
               throwable.getMessage().equals( ConnectionPool.CONNECTION_POOL_CLOSED_ERROR_MESSAGE );
    }

    private void writeAndFlush( TestkitResponse response )
    {
        if ( channel == null )
        {
            throw new IllegalStateException( "Called before channel is initialized" );
        }
        channel.writeAndFlush( response );
    }

    public enum BackendMode
    {
        SYNC,
        ASYNC,
        REACTIVE
    }
}
