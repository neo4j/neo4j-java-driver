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

import java.util.Map;

import org.neo4j.driver.internal.messaging.InitMessage;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;

public class HandshakeCompletedListener implements ChannelFutureListener
{
    private final String userAgent;
    private final Map<String,Value> authToken;
    private final ChannelPromise connectionInitializedPromise;

    public HandshakeCompletedListener( String userAgent, Map<String,Value> authToken,
            ChannelPromise connectionInitializedPromise )
    {
        this.userAgent = userAgent;
        this.authToken = authToken;
        this.connectionInitializedPromise = connectionInitializedPromise;
    }

    @Override
    public void operationComplete( ChannelFuture future ) throws Exception
    {
        if ( future.isSuccess() )
        {
            Channel channel = future.channel();

            InitMessage message = new InitMessage( userAgent, authToken );
            InitResponseHandler handler = new InitResponseHandler( connectionInitializedPromise );

            messageDispatcher( channel ).queue( handler );
            channel.writeAndFlush( message );
        }
        else
        {
            connectionInitializedPromise.setFailure( future.cause() );
        }
    }

    private static class InitResponseHandler implements ResponseHandler
    {
        final ChannelPromise connectionInitializedPromise;

        InitResponseHandler( ChannelPromise connectionInitializedPromise )
        {
            this.connectionInitializedPromise = connectionInitializedPromise;
        }

        @Override
        public void onSuccess( Map<String,Value> metadata )
        {
            // todo: process metadata, parse server version and adjust byte array support, etc.
            connectionInitializedPromise.setSuccess();
        }

        @Override
        public void onFailure( Throwable error )
        {
            connectionInitializedPromise.setFailure( error );
        }

        @Override
        public void onRecord( Value[] fields )
        {
            throw new UnsupportedOperationException();
        }
    }
}
