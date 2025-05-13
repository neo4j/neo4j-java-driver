/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package neo4j.org.testkit.backend;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import java.util.concurrent.Executors;
import neo4j.org.testkit.backend.channel.handler.HttpRequestHandler;
import neo4j.org.testkit.backend.handler.ReadyHandler;
import neo4j.org.testkit.backend.handler.WorkloadHandler;
import org.neo4j.driver.GraphDatabase;

public class Runner {
    public static void main(String[] args) throws InterruptedException {
        var config = Config.load();
        var driver = GraphDatabase.driver(config.uri(), config.authToken());

        EventLoopGroup group = new NioEventLoopGroup();
        var executor = Executors.newCachedThreadPool();
        var workloadHandler = new WorkloadHandler(driver, executor);
        var readyHandler = new ReadyHandler(driver);
        try {
            var bootstrap = new ServerBootstrap();
            bootstrap
                    .group(group)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(config.port())
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            var pipeline = channel.pipeline();
                            pipeline.addLast("codec", new HttpServerCodec());
                            pipeline.addLast("aggregator", new HttpObjectAggregator(512 * 1024));
                            pipeline.addLast(new HttpRequestHandler(workloadHandler, readyHandler));
                        }
                    });
            var server = bootstrap.bind().sync();
            server.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully().sync();
        }
    }
}
