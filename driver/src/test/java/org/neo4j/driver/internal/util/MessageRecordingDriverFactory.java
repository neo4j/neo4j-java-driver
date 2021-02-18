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
package org.neo4j.driver.internal.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.async.connection.ChannelConnector;
import org.neo4j.driver.internal.async.connection.ChannelConnectorImpl;
import org.neo4j.driver.internal.async.connection.ChannelPipelineBuilder;
import org.neo4j.driver.internal.async.connection.ChannelPipelineBuilderImpl;
import org.neo4j.driver.internal.async.outbound.OutboundMessageHandler;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.Config;
import org.neo4j.driver.Logging;

public class MessageRecordingDriverFactory extends DriverFactory
{
    private final Map<Channel,List<Message>> messagesByChannel = new ConcurrentHashMap<>();

    public Map<Channel,List<Message>> getMessagesByChannel()
    {
        return messagesByChannel;
    }

    @Override
    protected ChannelConnector createConnector( ConnectionSettings settings, SecurityPlan securityPlan, Config config, Clock clock,
                                                RoutingContext routingContext )
    {
        ChannelPipelineBuilder pipelineBuilder = new MessageRecordingChannelPipelineBuilder();
        return new ChannelConnectorImpl( settings, securityPlan, pipelineBuilder, config.logging(), clock, routingContext );
    }

    private class MessageRecordingChannelPipelineBuilder extends ChannelPipelineBuilderImpl
    {
        @Override
        public void build( MessageFormat messageFormat, ChannelPipeline pipeline, Logging logging )
        {
            super.build( messageFormat, pipeline, logging );
            pipeline.addAfter( OutboundMessageHandler.NAME, MessageRecordingHandler.class.getSimpleName(), new MessageRecordingHandler() );
        }
    }

    private class MessageRecordingHandler extends MessageToMessageEncoder<Message>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, Message msg, List<Object> out )
        {
            List<Message> messages = messagesByChannel.computeIfAbsent( ctx.channel(), ignore -> new CopyOnWriteArrayList<>() );
            messages.add( msg );
            out.add( msg );
        }
    }
}
