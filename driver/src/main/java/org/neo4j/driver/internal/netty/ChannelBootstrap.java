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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;

public class ChannelBootstrap implements AutoCloseable
{
    private final SecurityPlan securityPlan;
    private final Bootstrap bootstrap;

    public ChannelBootstrap( SecurityPlan securityPlan )
    {
        this.securityPlan = securityPlan;
        this.bootstrap = createBootstrap();
    }

    public ChannelFuture connect( final BoltServerAddress address )
    {
        bootstrap.handler( new ChannelInitializer<SocketChannel>()
        {
            @Override
            protected void initChannel( SocketChannel ch ) throws Exception
            {
                if ( securityPlan.requiresEncryption() )
                {
                    SSLEngine sslEngine = createSslEngine( address );
                    ch.pipeline().addLast( new SslHandler( sslEngine ) );
                }
            }
        } );

        return bootstrap.connect( address.toSocketAddress() );
    }

    @Override
    public void close() throws Exception
    {
        bootstrap.config().group().shutdownGracefully().sync();
    }

    private SSLEngine createSslEngine( BoltServerAddress address ) throws SSLException
    {
        SSLContext sslContext = securityPlan.sslContext();
        SSLEngine sslEngine = sslContext.createSSLEngine( address.host(), address.port() );
        sslEngine.setUseClientMode( true );
        return sslEngine;
    }

    private static Bootstrap createBootstrap()
    {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group( new NioEventLoopGroup() );
        bootstrap.channel( NioSocketChannel.class );
        return bootstrap;
    }
}
