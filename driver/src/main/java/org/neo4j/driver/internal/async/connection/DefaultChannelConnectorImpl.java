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
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.Clock;

import static java.util.Objects.requireNonNull;

public class DefaultChannelConnectorImpl extends AbstractChannelConnectorImpl
{
    private final SecurityPlan securityPlan;
    
    public DefaultChannelConnectorImpl( ConnectionSettings connectionSettings, SecurityPlan securityPlan, Logging logging, Clock clock )
    {
        super( connectionSettings, logging, clock );
        this.securityPlan = requireNonNull( securityPlan );
    }

    public DefaultChannelConnectorImpl( ConnectionSettings connectionSettings, SecurityPlan securityPlan, ChannelPipelineBuilder pipelineBuilder,
            Logging logging, Clock clock )
    {
        super( connectionSettings, pipelineBuilder, logging, clock );
        this.securityPlan = requireNonNull( securityPlan );
    }

    @Override
    protected ChannelInitializer<Channel> createNettyChannelInitializer( BoltServerAddress address, int connectTimeoutMillis, Clock clock, Logging logging )
    {
        return new DefaultNettyChannelInitializer( address, securityPlan, connectTimeoutMillis, clock, logging );
    }
}
