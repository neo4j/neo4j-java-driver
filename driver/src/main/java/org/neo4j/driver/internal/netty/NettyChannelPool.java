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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static java.util.Objects.requireNonNull;

public class NettyChannelPool extends FixedChannelPool
{
    private static final int MAX_PENDING_ACQUIRES = Integer.MAX_VALUE;
    private static final boolean RELEASE_HEALTH_CHECK = true;

    private final BoltServerAddress address;
    private final AsyncConnector connector;

    public NettyChannelPool( BoltServerAddress address, AsyncConnector connector, Bootstrap bootstrap,
            ChannelPoolHandler handler, ChannelHealthChecker healthCheck, long acquireTimeoutMillis,
            int maxConnections )
    {
        super( bootstrap, handler, healthCheck, AcquireTimeoutAction.FAIL, acquireTimeoutMillis, maxConnections,
                MAX_PENDING_ACQUIRES, RELEASE_HEALTH_CHECK );

        this.address = requireNonNull( address );
        this.connector = requireNonNull( connector );
    }

    @Override
    protected ChannelFuture connectChannel( Bootstrap bootstrap )
    {
        return connector.connect( address, bootstrap );
    }
}
