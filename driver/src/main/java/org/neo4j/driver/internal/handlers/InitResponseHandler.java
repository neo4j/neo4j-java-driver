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
package org.neo4j.driver.internal.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;

import java.util.Map;

import org.neo4j.driver.internal.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.Value;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setServerVersion;
import static org.neo4j.driver.internal.util.MetadataExtractor.extractNeo4jServerVersion;

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
            ServerVersion serverVersion = extractNeo4jServerVersion( metadata );
            setServerVersion( channel, serverVersion );
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
}
