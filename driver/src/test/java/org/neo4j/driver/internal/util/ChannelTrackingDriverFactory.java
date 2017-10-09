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
package org.neo4j.driver.internal.util;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.async.AsyncConnector;
import org.neo4j.driver.internal.async.BoltServerAddress;
import org.neo4j.driver.internal.async.pool.AsyncConnectionPool;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;

public class ChannelTrackingDriverFactory extends DriverFactoryWithClock
{
    private final List<Channel> channels = new CopyOnWriteArrayList<>();
    private AsyncConnectionPool pool;

    public ChannelTrackingDriverFactory( Clock clock )
    {
        super( clock );
    }

    @Override
    protected AsyncConnector createConnector( ConnectionSettings settings, SecurityPlan securityPlan, Config config,
            Clock clock )
    {
        AsyncConnector connector = super.createConnector( settings, securityPlan, config, clock );
        return new ChannelTrackingConnector( connector, channels );
    }

    @Override
    protected AsyncConnectionPool createConnectionPool( AuthToken authToken, SecurityPlan securityPlan,
            Bootstrap bootstrap, Config config )
    {
        pool = super.createConnectionPool( authToken, securityPlan, bootstrap, config );
        return pool;
    }

    public List<Channel> channels()
    {
        return new ArrayList<>( channels );
    }

    public void closeChannels()
    {
        for ( Channel channel : channels )
        {
            channel.close().syncUninterruptibly();
        }
        channels.clear();
    }

    public int activeChannels( BoltServerAddress address )
    {
        return pool == null ? 0 : pool.activeConnections( address );
    }
}
