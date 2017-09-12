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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.pool.ChannelPoolHandler;

import java.util.Map;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Objects.requireNonNull;

public class AsyncConnectorImpl implements AsyncConnector
{
    private final String userAgent;
    private final Map<String,Value> authToken;
    private final SecurityPlan securityPlan;
    private final ChannelPoolHandler channelPoolHandler;
    private final Logging logging;
    private final Clock clock;

    public AsyncConnectorImpl( ConnectionSettings connectionSettings, SecurityPlan securityPlan,
            ChannelPoolHandler channelPoolHandler, Logging logging, Clock clock )
    {
        this.userAgent = connectionSettings.userAgent();
        this.authToken = tokenAsMap( connectionSettings.authToken() );
        this.securityPlan = requireNonNull( securityPlan );
        this.channelPoolHandler = requireNonNull( channelPoolHandler );
        this.logging = requireNonNull( logging );
        this.clock = requireNonNull( clock );
    }

    @Override
    public ChannelFuture connect( BoltServerAddress address, Bootstrap bootstrap )
    {
        bootstrap.handler( new NettyChannelInitializer( address, securityPlan, channelPoolHandler, clock ) );

        ChannelFuture channelConnected = bootstrap.connect( address.toSocketAddress() );

        Channel channel = channelConnected.channel();
        ChannelPromise handshakeCompleted = channel.newPromise();
        ChannelPromise connectionInitialized = channel.newPromise();

        channelConnected.addListener( new ChannelConnectedListener( address, handshakeCompleted, logging ) );
        handshakeCompleted.addListener( new HandshakeCompletedListener( userAgent, authToken, connectionInitialized ) );

        return connectionInitialized;
    }

    private static Map<String,Value> tokenAsMap( AuthToken token )
    {
        if ( token instanceof InternalAuthToken )
        {
            return ((InternalAuthToken) token).toMap();
        }
        else
        {
            throw new ClientException(
                    "Unknown authentication token, `" + token + "`. Please use one of the supported " +
                    "tokens from `" + AuthTokens.class.getSimpleName() + "`." );
        }
    }
}
