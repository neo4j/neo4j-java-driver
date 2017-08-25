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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;

import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.Value;

import static java.util.Objects.requireNonNull;

public class Connector implements AutoCloseable
{
    private final String userAgent;
    private final Map<String,Value> authToken;
    private final ChannelBootstrap bootstrap;

    public Connector( String userAgent, Map<String,Value> authToken )
    {
        this.userAgent = requireNonNull( userAgent );
        this.authToken = requireNonNull( authToken );
        this.bootstrap = new ChannelBootstrap();
    }

    public AsyncConnection connect( BoltServerAddress address )
    {
        ChannelFuture channelConnected = bootstrap.connect( address );

        Channel channel = channelConnected.channel();
        ChannelPromise handshakeCompleted = channel.newPromise();
        ChannelPromise connectionInitialized = channel.newPromise();

        channelConnected.addListener( new ChannelConnectedListener( address, handshakeCompleted ) );
        handshakeCompleted.addListener( new HandshakeCompletedListener( userAgent, authToken, connectionInitialized ) );

        return new NettyConnection( connectionInitialized );
    }

    @Override
    public void close() throws Exception
    {
        bootstrap.close();
    }
}
