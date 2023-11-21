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
package org.neo4j.driver.internal.util.io;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.BoltAgent;
import org.neo4j.driver.internal.BoltAgentUtil;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.DriverFactoryWithClock;

public class ChannelTrackingDriverFactory extends DriverFactoryWithClock {
    private final List<Channel> channels = new CopyOnWriteArrayList<>();
    private final int eventLoopThreads;
    private ConnectionPool pool;

    public ChannelTrackingDriverFactory(Clock clock) {
        this(0, clock);
    }

    public ChannelTrackingDriverFactory(int eventLoopThreads, Clock clock) {
        super(clock);
        this.eventLoopThreads = eventLoopThreads;
    }

    @Override
    protected Bootstrap createBootstrap(int size) {
        return BootstrapFactory.newBootstrap(eventLoopThreads);
    }

    @Override
    protected final ChannelConnector createConnector(
            ConnectionSettings settings,
            SecurityPlan securityPlan,
            Config config,
            Clock clock,
            RoutingContext routingContext,
            BoltAgent boltAgent) {
        return createChannelTrackingConnector(
                createRealConnector(settings, securityPlan, config, clock, routingContext));
    }

    @Override
    protected final ConnectionPool createConnectionPool(
            AuthTokenManager authTokenManager,
            SecurityPlan securityPlan,
            Bootstrap bootstrap,
            MetricsProvider metricsProvider,
            Config config,
            boolean ownsEventLoopGroup,
            RoutingContext routingContext) {
        pool = super.createConnectionPool(
                authTokenManager, securityPlan, bootstrap, metricsProvider, config, ownsEventLoopGroup, routingContext);
        return pool;
    }

    protected ChannelConnector createRealConnector(
            ConnectionSettings settings,
            SecurityPlan securityPlan,
            Config config,
            Clock clock,
            RoutingContext routingContext) {
        return super.createConnector(settings, securityPlan, config, clock, routingContext, BoltAgentUtil.VALUE);
    }

    private ChannelTrackingConnector createChannelTrackingConnector(ChannelConnector connector) {
        return new ChannelTrackingConnector(connector, channels);
    }

    public List<Channel> channels() {
        return new ArrayList<>(channels);
    }

    public int activeChannels(BoltServerAddress address) {
        return pool == null ? 0 : pool.inUseConnections(address);
    }
}
