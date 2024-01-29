/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static java.util.Objects.requireNonNull;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.resolver.AddressResolverGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.internal.BoltAgent;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DomainNameResolver;
import org.neo4j.driver.internal.async.inbound.ConnectTimeoutHandler;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.security.SecurityPlan;

public class ChannelConnectorImpl implements ChannelConnector {
    private final String userAgent;
    private final BoltAgent boltAgent;
    private final AuthTokenManager authTokenManager;
    private final RoutingContext routingContext;
    private final SecurityPlan securityPlan;
    private final ChannelPipelineBuilder pipelineBuilder;
    private final int connectTimeoutMillis;
    private final Logging logging;
    private final Clock clock;
    private final DomainNameResolver domainNameResolver;
    private final AddressResolverGroup<InetSocketAddress> addressResolverGroup;
    private final NotificationConfig notificationConfig;

    public ChannelConnectorImpl(
            ConnectionSettings connectionSettings,
            SecurityPlan securityPlan,
            Logging logging,
            Clock clock,
            RoutingContext routingContext,
            DomainNameResolver domainNameResolver,
            NotificationConfig notificationConfig,
            BoltAgent boltAgent) {
        this(
                connectionSettings,
                securityPlan,
                new ChannelPipelineBuilderImpl(),
                logging,
                clock,
                routingContext,
                domainNameResolver,
                notificationConfig,
                boltAgent);
    }

    public ChannelConnectorImpl(
            ConnectionSettings connectionSettings,
            SecurityPlan securityPlan,
            ChannelPipelineBuilder pipelineBuilder,
            Logging logging,
            Clock clock,
            RoutingContext routingContext,
            DomainNameResolver domainNameResolver,
            NotificationConfig notificationConfig,
            BoltAgent boltAgent) {
        this.userAgent = connectionSettings.userAgent();
        this.boltAgent = requireNonNull(boltAgent);
        this.authTokenManager = connectionSettings.authTokenProvider();
        this.routingContext = routingContext;
        this.connectTimeoutMillis = connectionSettings.connectTimeoutMillis();
        this.securityPlan = requireNonNull(securityPlan);
        this.pipelineBuilder = pipelineBuilder;
        this.logging = requireNonNull(logging);
        this.clock = requireNonNull(clock);
        this.domainNameResolver = requireNonNull(domainNameResolver);
        this.addressResolverGroup = new NettyDomainNameResolverGroup(this.domainNameResolver);
        this.notificationConfig = notificationConfig;
    }

    @Override
    public ChannelFuture connect(BoltServerAddress address, Bootstrap bootstrap) {
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis);
        bootstrap.handler(new NettyChannelInitializer(
                address, securityPlan, connectTimeoutMillis, authTokenManager, clock, logging));
        bootstrap.resolver(addressResolverGroup);

        SocketAddress socketAddress;
        try {
            socketAddress =
                    new InetSocketAddress(domainNameResolver.resolve(address.connectionHost())[0], address.port());
        } catch (Throwable t) {
            socketAddress = InetSocketAddress.createUnresolved(address.connectionHost(), address.port());
        }

        var channelConnected = bootstrap.connect(socketAddress);

        var channel = channelConnected.channel();
        var handshakeCompleted = channel.newPromise();
        var connectionInitialized = channel.newPromise();

        installChannelConnectedListeners(address, channelConnected, handshakeCompleted);
        installHandshakeCompletedListeners(handshakeCompleted, connectionInitialized);

        return connectionInitialized;
    }

    private void installChannelConnectedListeners(
            BoltServerAddress address, ChannelFuture channelConnected, ChannelPromise handshakeCompleted) {
        var pipeline = channelConnected.channel().pipeline();

        // add timeout handler to the pipeline when channel is connected. it's needed to limit amount of time code
        // spends in TLS and Bolt handshakes. prevents infinite waiting when database does not respond
        channelConnected.addListener(future -> pipeline.addFirst(new ConnectTimeoutHandler(connectTimeoutMillis)));

        // add listener that sends Bolt handshake bytes when channel is connected
        channelConnected.addListener(
                new ChannelConnectedListener(address, pipelineBuilder, handshakeCompleted, logging));
    }

    private void installHandshakeCompletedListeners(
            ChannelPromise handshakeCompleted, ChannelPromise connectionInitialized) {
        var pipeline = handshakeCompleted.channel().pipeline();

        // remove timeout handler from the pipeline once TLS and Bolt handshakes are completed. regular protocol
        // messages will flow next and we do not want to have read timeout for them
        handshakeCompleted.addListener(future -> {
            if (future.isSuccess()) {
                pipeline.remove(ConnectTimeoutHandler.class);
            }
        });

        // add listener that sends an INIT message. connection is now fully established. channel pipeline if fully
        // set to send/receive messages for a selected protocol version
        handshakeCompleted.addListener(new HandshakeCompletedListener(
                userAgent, boltAgent, routingContext, connectionInitialized, notificationConfig, clock));
    }
}
