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
package org.neo4j.driver.internal.async;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.Clock;

import static org.neo4j.driver.internal.async.ChannelAttributes.setAddress;
import static org.neo4j.driver.internal.async.ChannelAttributes.setCreationTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.setMessageDispatcher;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

public class NettyChannelInitializer extends ChannelInitializer<Channel>
{
    private final BoltServerAddress address;
    private final SecurityPlan securityPlan;
    private final ChannelPoolHandler channelPoolHandler;
    private final Clock clock;

    public NettyChannelInitializer( BoltServerAddress address, SecurityPlan securityPlan,
            ChannelPoolHandler channelPoolHandler, Clock clock )
    {
        this.address = address;
        this.securityPlan = securityPlan;
        this.channelPoolHandler = channelPoolHandler;
        this.clock = clock;
    }

    @Override
    protected void initChannel( Channel channel ) throws Exception
    {
        if ( securityPlan.requiresEncryption() )
        {
            SslHandler sslHandler = createSslHandler();
            channel.pipeline().addFirst( sslHandler );
        }

        updateChannelAttributes( channel );

        channelPoolHandler.channelCreated( channel );
    }

    private SslHandler createSslHandler() throws SSLException
    {
        SSLEngine sslEngine = createSslEngine();
        return new SslHandler( sslEngine );
    }

    private SSLEngine createSslEngine() throws SSLException
    {
        SSLContext sslContext = securityPlan.sslContext();
        SSLEngine sslEngine = sslContext.createSSLEngine( address.host(), address.port() );
        sslEngine.setUseClientMode( true );
        return sslEngine;
    }

    private void updateChannelAttributes( Channel channel )
    {
        setAddress( channel, address );
        setCreationTimestamp( channel, clock.millis() );
        setMessageDispatcher( channel, new InboundMessageDispatcher( channel, DEV_NULL_LOGGING ) );
    }
}
