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
package org.neo4j.driver.internal.util.io;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.util.List;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelConnector;

public class ChannelTrackingConnector implements ChannelConnector
{
    private final ChannelConnector realConnector;
    private final List<Channel> channels;

    public ChannelTrackingConnector( ChannelConnector realConnector, List<Channel> channels )
    {
        this.realConnector = realConnector;
        this.channels = channels;
    }

    @Override
    public ChannelFuture connect( BoltServerAddress address, Bootstrap bootstrap )
    {
        ChannelFuture channelFuture = realConnector.connect( address, bootstrap );
        channels.add( channelFuture.channel() );
        return channelFuture;
    }
}
