/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslHandler;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.async.BoltProtocolUtil.BOLT_MAGIC_PREAMBLE;

class EncryptionTest
{
    @Test
    void shouldOperateWithNoEncryptionWhenItIsOptionalInTheDatabase() throws Throwable
    {
        testDriverEncryption( false );
    }

    @Test
    void shouldOperateWithEncryptionWhenItIsOptionalInTheDatabase() throws Throwable
    {
        testDriverEncryption( true );
    }

    private void testDriverEncryption( boolean isEncrypted ) throws Throwable
    {
        DummyNettyServer server = new DummyNettyServer();
        int port = server.start();

        Config config = newConfig( isEncrypted );
        RuntimeException e = assertThrows( RuntimeException.class, () -> GraphDatabase.driver( "bolt://localhost:" + port, config ).close() );
        server.stop();

        server.assertClientEncryption( isEncrypted );
        assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        assertThat( e.getMessage(), startsWith( "Connection to the database terminated" ) );
    }

    private static Config newConfig( boolean withEncryption )
    {
        return withEncryption ? configWithEncryption() : configWithoutEncryption();
    }

    private static Config configWithEncryption()
    {
        return Config.build().withEncryption().toConfig();
    }

    private static Config configWithoutEncryption()
    {
        return Config.build().withoutEncryption().toConfig();
    }

    enum ClientSocketStatus
    {
        Encrypted,
        Unencrypted,
        Unknown,
        FailedToRecognize
    }

    public final class DummyNettyServer
    {
        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;
        private Channel serverChannel;
        private final AtomicReference<ClientSocketStatus> clientStatus = new AtomicReference<>( ClientSocketStatus.Unknown );

        public int start() throws Throwable
        {
            bossGroup = new NioEventLoopGroup( 1 );
            workerGroup = new NioEventLoopGroup();

            ServerBootstrap server = new ServerBootstrap();
            server.group( bossGroup, workerGroup ).channel( NioServerSocketChannel.class )
                    .childHandler( new ChannelInitializer<SocketChannel>()
            {
                @Override
                public void initChannel( SocketChannel ch ) throws Exception
                {
                    ChannelPipeline p = ch.pipeline();
                    p.addLast( new ByteToMessageDecoder()
                    {
                        @Override
                        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> list ) throws Exception
                        {
                            // Will use the first five bytes to detect a protocol.
                            if ( in.readableBytes() < 5 )
                            {
                                return;
                            }

                            checkClientSocketStatus( in );
                            ctx.close();
                        }

                        @Override
                        public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
                        {
                            // Close the connection when an exception is raised.
                            cause.printStackTrace();
                            ctx.close();
                        }
                    } );
                }
            } );
            // Start the server.
            serverChannel = server.bind( 0 ).sync().channel();
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }

        public void stop() throws Throwable
        {
            try
            {
                // Now shutting down
                serverChannel.close().sync();
            }
            finally
            {
                // Shut down all event loops to terminate all threads.
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        }

        private void checkClientSocketStatus( ByteBuf buffer )
        {
            if ( SslHandler.isEncrypted( buffer ) )
            {
                clientStatus.compareAndSet( ClientSocketStatus.Unknown, ClientSocketStatus.Encrypted );
            }
            else if ( buffer.readInt() == BOLT_MAGIC_PREAMBLE )
            {
                clientStatus.compareAndSet( ClientSocketStatus.Unknown, ClientSocketStatus.Unencrypted );
            }
            else
            {
                clientStatus.compareAndSet( ClientSocketStatus.Unknown, ClientSocketStatus.FailedToRecognize );
            }
        }

        public void assertClientEncryption( boolean isEncrypted )
        {
            if ( isEncrypted )
            {
                assertThat( clientStatus.get(), equalTo( ClientSocketStatus.Encrypted ) );
            }
            else
            {
                assertThat( clientStatus.get(), equalTo( ClientSocketStatus.Unencrypted ) );
            }
        }
    }
}
