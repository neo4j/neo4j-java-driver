/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;

import java.util.Map;

import org.neo4j.driver.internal.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Value;

import static org.neo4j.driver.internal.async.ChannelAttributes.setServerVersion;

public class InitResponseHandler implements ResponseHandler
{
    private final ChannelPromise connectionInitializedPromise;
    private final Channel channel;

    public InitResponseHandler( ChannelPromise connectionInitializedPromise )
    {
        this.connectionInitializedPromise = connectionInitializedPromise;
        this.channel = connectionInitializedPromise.channel();
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        try
        {
            ServerVersion serverVersion = extractServerVersion( metadata );
            setServerVersion( channel, serverVersion );
            updatePipelineIfNeeded( serverVersion, channel.pipeline() );
            connectionInitializedPromise.setSuccess();
        }
        catch ( Throwable error )
        {
            connectionInitializedPromise.setFailure( error );
            throw error;
        }
    }

    @Override
    public void onFailure( Throwable error )
    {
        channel.close().addListener( future -> connectionInitializedPromise.setFailure( error ) );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException();
    }

    private static ServerVersion extractServerVersion( Map<String,Value> metadata )
    {
        Value versionValue = metadata.get( "server" );
        boolean versionAbsent = versionValue == null || versionValue.isNull();
        return versionAbsent ? ServerVersion.v3_0_0 : ServerVersion.version( versionValue.asString() );
    }

    private static void updatePipelineIfNeeded( ServerVersion serverVersion, ChannelPipeline pipeline )
    {
        if ( serverVersion.lessThan( ServerVersion.v3_2_0 ) )
        {
            OutboundMessageHandler outboundHandler = pipeline.get( OutboundMessageHandler.class );
            if ( outboundHandler == null )
            {
                throw new IllegalStateException( "Can't find " + OutboundMessageHandler.NAME + " in the pipeline" );
            }
            pipeline.replace( outboundHandler, OutboundMessageHandler.NAME, outboundHandler.withoutByteArraySupport() );
        }
    }
}
