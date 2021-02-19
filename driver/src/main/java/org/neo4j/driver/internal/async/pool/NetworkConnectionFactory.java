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
package org.neo4j.driver.internal.async.pool;

import io.netty.channel.Channel;

import org.neo4j.driver.internal.async.NetworkConnection;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;

public class NetworkConnectionFactory implements ConnectionFactory
{
    private final Clock clock;
    private final MetricsListener metricsListener;

    public NetworkConnectionFactory( Clock clock, MetricsListener metricsListener )
    {
        this.clock = clock;
        this.metricsListener = metricsListener;
    }

    @Override
    public Connection createConnection( Channel channel, ExtendedChannelPool pool )
    {
        return new NetworkConnection( channel, pool, clock, metricsListener );
    }
}
