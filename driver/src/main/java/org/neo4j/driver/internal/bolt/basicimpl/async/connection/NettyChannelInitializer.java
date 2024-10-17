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
package org.neo4j.driver.internal.bolt.basicimpl.async.connection;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.ssl.SslHandler;
import java.time.Clock;
import javax.net.ssl.SSLEngine;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;

public class NettyChannelInitializer extends ChannelInitializer<Channel> {
    private final BoltServerAddress address;
    private final SecurityPlan securityPlan;
    private final int connectTimeoutMillis;
    private final Clock clock;
    private final LoggingProvider logging;

    public NettyChannelInitializer(
            BoltServerAddress address,
            SecurityPlan securityPlan,
            int connectTimeoutMillis,
            Clock clock,
            LoggingProvider logging) {
        this.address = address;
        this.securityPlan = securityPlan;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.clock = clock;
        this.logging = logging;
    }

    @Override
    protected void initChannel(Channel channel) {
        if (securityPlan.requiresEncryption()) {
            var sslHandler = createSslHandler();
            channel.pipeline().addFirst(sslHandler);
        }

        updateChannelAttributes(channel);
    }

    private SslHandler createSslHandler() {
        var sslEngine = createSslEngine();
        var sslHandler = new SslHandler(sslEngine);
        sslHandler.setHandshakeTimeoutMillis(connectTimeoutMillis);
        return sslHandler;
    }

    private SSLEngine createSslEngine() {
        var sslContext = securityPlan.sslContext();
        var sslEngine = sslContext.createSSLEngine(address.host(), address.port());
        sslEngine.setNeedClientAuth(securityPlan.requiresClientAuth());
        sslEngine.setUseClientMode(true);
        if (securityPlan.requiresHostnameVerification()) {
            var sslParameters = sslEngine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            sslEngine.setSSLParameters(sslParameters);
        }
        return sslEngine;
    }

    private void updateChannelAttributes(Channel channel) {
        ChannelAttributes.setServerAddress(channel, address);
        ChannelAttributes.setCreationTimestamp(channel, clock.millis());
        ChannelAttributes.setMessageDispatcher(channel, new InboundMessageDispatcher(channel, logging));
    }
}
