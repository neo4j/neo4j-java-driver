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
package org.neo4j.driver.internal.async.connection;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;

import java.util.Map;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.async.inbound.ConnectTimeoutHandler;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;

import static java.util.Objects.requireNonNull;

public class ChannelConnectorImpl implements ChannelConnector
{
    private final String userAgent;
    private final AuthToken authToken;
    private final RoutingContext routingContext;
    private final SecurityPlan securityPlan;
    private final ChannelPipelineBuilder pipelineBuilder;
    private final int connectTimeoutMillis;
    private final Logging logging;
    private final Clock clock;

    public ChannelConnectorImpl( ConnectionSettings connectionSettings, SecurityPlan securityPlan, Logging logging,
            Clock clock, RoutingContext routingContext )
    {
        this( connectionSettings, securityPlan, new ChannelPipelineBuilderImpl(), logging, clock, routingContext );
    }

    public ChannelConnectorImpl( ConnectionSettings connectionSettings, SecurityPlan securityPlan,
            ChannelPipelineBuilder pipelineBuilder, Logging logging, Clock clock, RoutingContext routingContext )
    {
        this.userAgent = connectionSettings.userAgent();
        this.authToken = requireValidAuthToken( connectionSettings.authToken() );
        this.routingContext = routingContext;
        this.connectTimeoutMillis = connectionSettings.connectTimeoutMillis();
        this.securityPlan = requireNonNull( securityPlan );
        this.pipelineBuilder = pipelineBuilder;
        this.logging = requireNonNull( logging );
        this.clock = requireNonNull( clock );
    }

    @Override
    public ChannelFuture connect( BoltServerAddress address, Bootstrap bootstrap )
    {
        bootstrap.option( ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis );
        bootstrap.handler( new NettyChannelInitializer( address, securityPlan, connectTimeoutMillis, clock, logging ) );

        ChannelFuture channelConnected = bootstrap.connect( address.toSocketAddress() );

        Channel channel = channelConnected.channel();
        ChannelPromise handshakeCompleted = channel.newPromise();
        ChannelPromise connectionInitialized = channel.newPromise();

        installChannelConnectedListeners( address, channelConnected, handshakeCompleted );
        installHandshakeCompletedListeners( handshakeCompleted, connectionInitialized );

        return connectionInitialized;
    }

    private void installChannelConnectedListeners( BoltServerAddress address, ChannelFuture channelConnected,
            ChannelPromise handshakeCompleted )
    {
        ChannelPipeline pipeline = channelConnected.channel().pipeline();

        // add timeout handler to the pipeline when channel is connected. it's needed to limit amount of time code
        // spends in TLS and Bolt handshakes. prevents infinite waiting when database does not respond
        channelConnected.addListener( future ->
                pipeline.addFirst( new ConnectTimeoutHandler( connectTimeoutMillis ) ) );

        // add listener that sends Bolt handshake bytes when channel is connected
        channelConnected.addListener(
                new ChannelConnectedListener( address, pipelineBuilder, handshakeCompleted, logging ) );
    }

    private void installHandshakeCompletedListeners( ChannelPromise handshakeCompleted,
            ChannelPromise connectionInitialized )
    {
        ChannelPipeline pipeline = handshakeCompleted.channel().pipeline();

        // remove timeout handler from the pipeline once TLS and Bolt handshakes are completed. regular protocol
        // messages will flow next and we do not want to have read timeout for them
        handshakeCompleted.addListener( future -> pipeline.remove( ConnectTimeoutHandler.class ) );

        // add listener that sends an INIT message. connection is now fully established. channel pipeline if fully
        // set to send/receive messages for a selected protocol version
        handshakeCompleted.addListener( new HandshakeCompletedListener( userAgent, authToken, routingContext, connectionInitialized ) );
    }

    private static AuthToken requireValidAuthToken( AuthToken token )
    {
        if ( token instanceof InternalAuthToken )
        {
            return token;
        }
        else
        {
            throw new ClientException(
                    "Unknown authentication token, `" + token + "`. Please use one of the supported " +
                    "tokens from `" + AuthTokens.class.getSimpleName() + "`." );
        }
    }
}
