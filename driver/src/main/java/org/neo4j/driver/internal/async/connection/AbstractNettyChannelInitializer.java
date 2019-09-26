/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.util.Clock;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setCreationTimestamp;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setServerAddress;

public abstract class AbstractNettyChannelInitializer extends ChannelInitializer<Channel>
{
    private final BoltServerAddress address;
    private final Clock clock;
    private final Logging logging;

    protected AbstractNettyChannelInitializer( BoltServerAddress address, Clock clock, Logging logging )
    {
        this.address = address;
        this.clock = clock;
        this.logging = logging;
    }

    @Override
    protected void initChannel( Channel channel )
    {
        addSslHandler( channel );
        updateChannelAttributes( channel );
    }

    protected abstract void addSslHandler( Channel channel );

    private void updateChannelAttributes( Channel channel )
    {
        setServerAddress( channel, address );
        setCreationTimestamp( channel, clock.millis() );
        setMessageDispatcher( channel, new InboundMessageDispatcher( channel, logging ) );
    }
}
