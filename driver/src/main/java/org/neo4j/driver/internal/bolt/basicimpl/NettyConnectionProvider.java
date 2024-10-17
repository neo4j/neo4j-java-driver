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
package org.neo4j.driver.internal.bolt.basicimpl;

import static java.util.Objects.requireNonNull;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.resolver.AddressResolverGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.MetricsListener;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.RoutingContext;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.basicimpl.async.NetworkConnection;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelConnectedListener;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelPipelineBuilderImpl;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.HandshakeCompletedListener;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.NettyChannelInitializer;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.NettyDomainNameResolverGroup;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.ConnectTimeoutHandler;
import org.neo4j.driver.internal.bolt.basicimpl.spi.Connection;

public final class NettyConnectionProvider implements ConnectionProvider {
    private final EventLoopGroup eventLoopGroup;
    private final Clock clock;
    private final DomainNameResolver domainNameResolver;
    private final AddressResolverGroup<InetSocketAddress> addressResolverGroup;
    private final LocalAddress localAddress;

    private final LoggingProvider logging;

    public NettyConnectionProvider(
            EventLoopGroup eventLoopGroup,
            Clock clock,
            DomainNameResolver domainNameResolver,
            LocalAddress localAddress,
            LoggingProvider logging) {
        this.eventLoopGroup = eventLoopGroup;
        this.clock = clock;
        this.domainNameResolver = requireNonNull(domainNameResolver);
        this.addressResolverGroup = new NettyDomainNameResolverGroup(this.domainNameResolver);
        this.localAddress = localAddress;
        this.logging = logging;
    }

    @Override
    public CompletionStage<Connection> acquireConnection(
            BoltServerAddress address,
            SecurityPlan securityPlan,
            RoutingContext routingContext,
            String databaseName,
            Map<String, Value> authMap,
            BoltAgent boltAgent,
            String userAgent,
            AccessMode mode,
            int connectTimeoutMillis,
            String impersonatedUser,
            CompletableFuture<Long> latestAuthMillisFuture,
            NotificationConfig notificationConfig,
            MetricsListener metricsListener) {
        var bootstrap = new Bootstrap();
        bootstrap
                .group(this.eventLoopGroup)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                .channel(localAddress != null ? LocalChannel.class : NioSocketChannel.class)
                .resolver(addressResolverGroup)
                .handler(new NettyChannelInitializer(address, securityPlan, connectTimeoutMillis, clock, logging));

        SocketAddress socketAddress;
        if (localAddress == null) {
            try {
                socketAddress =
                        new InetSocketAddress(domainNameResolver.resolve(address.connectionHost())[0], address.port());
            } catch (Throwable t) {
                socketAddress = InetSocketAddress.createUnresolved(address.connectionHost(), address.port());
            }
        } else {
            socketAddress = localAddress;
        }

        var connectedFuture = bootstrap.connect(socketAddress);

        var channel = connectedFuture.channel();
        var handshakeCompleted = channel.newPromise();
        var connectionInitialized = channel.newPromise();

        installChannelConnectedListeners(address, connectedFuture, handshakeCompleted, connectTimeoutMillis);
        installHandshakeCompletedListeners(
                handshakeCompleted,
                connectionInitialized,
                address,
                routingContext,
                authMap,
                boltAgent,
                userAgent,
                latestAuthMillisFuture,
                notificationConfig);

        var future = new CompletableFuture<Connection>();
        connectionInitialized.addListener((ChannelFutureListener) f -> {
            var throwable = f.cause();
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                var connection = new NetworkConnection(channel, logging);
                future.complete(connection);
            }
        });
        return future;
    }

    private void installChannelConnectedListeners(
            BoltServerAddress address,
            ChannelFuture channelConnected,
            ChannelPromise handshakeCompleted,
            int connectTimeoutMillis) {
        var pipeline = channelConnected.channel().pipeline();

        // add timeout handler to the pipeline when channel is connected. it's needed to
        // limit amount of time code
        // spends in TLS and Bolt handshakes. prevents infinite waiting when database does
        // not respond
        channelConnected.addListener(future -> pipeline.addFirst(new ConnectTimeoutHandler(connectTimeoutMillis)));

        // add listener that sends Bolt handshake bytes when channel is connected
        channelConnected.addListener(
                new ChannelConnectedListener(address, new ChannelPipelineBuilderImpl(), handshakeCompleted, logging));
    }

    private void installHandshakeCompletedListeners(
            ChannelPromise handshakeCompleted,
            ChannelPromise connectionInitialized,
            BoltServerAddress address,
            RoutingContext routingContext,
            Map<String, Value> authMap,
            BoltAgent boltAgent,
            String userAgent,
            CompletableFuture<Long> latestAuthMillisFuture,
            NotificationConfig notificationConfig) {
        var pipeline = handshakeCompleted.channel().pipeline();

        // remove timeout handler from the pipeline once TLS and Bolt handshakes are
        // completed. regular protocol
        // messages will flow next and we do not want to have read timeout for them
        handshakeCompleted.addListener(future -> {
            if (future.isSuccess()) {
                pipeline.remove(ConnectTimeoutHandler.class);
            }
        });

        // add listener that sends an INIT message. connection is now fully established.
        // channel pipeline is fully
        // set to send/receive messages for a selected protocol version
        handshakeCompleted.addListener(new HandshakeCompletedListener(
                authMap,
                userAgent,
                boltAgent,
                routingContext,
                connectionInitialized,
                notificationConfig,
                this.clock,
                latestAuthMillisFuture));
    }
}
