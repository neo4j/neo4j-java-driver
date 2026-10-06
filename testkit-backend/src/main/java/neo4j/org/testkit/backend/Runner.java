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
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import neo4j.org.testkit.backend.channel.handler.TestkitMessageInboundHandler;
import neo4j.org.testkit.backend.channel.handler.TestkitMessageOutboundHandler;
import neo4j.org.testkit.backend.channel.handler.TestkitRequestProcessorHandler;
import neo4j.org.testkit.backend.channel.handler.TestkitRequestResponseMapperHandler;

public class Runner {
    public static void main(String[] args) throws InterruptedException {
        var backendMode = getBackendMode(args);

        var threadCounter = new AtomicInteger(0);
        var requestExecutorService =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), runnable -> {
                    var thread = new Thread(
                            runnable, "neo4j-testkit-request-executor-%d".formatted(threadCounter.getAndIncrement()));
                    thread.setDaemon(true);
                    return thread;
                });

        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        try {
            var bootstrap = new ServerBootstrap();

            bootstrap
                    .group(group)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(9876)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            var responseQueueHanlder = new ResponseQueueHanlder(channel::writeAndFlush);
                            channel.pipeline().addLast(new TestkitMessageInboundHandler());
                            channel.pipeline().addLast(new TestkitMessageOutboundHandler());
                            channel.pipeline().addLast(new TestkitRequestResponseMapperHandler(responseQueueHanlder));
                            channel.pipeline()
                                    .addLast(new TestkitRequestProcessorHandler(
                                            backendMode, responseQueueHanlder, requestExecutorService));
                        }
                    });
            var server = bootstrap.bind().sync();
            server.channel().closeFuture().sync();
        } finally {
            requestExecutorService.shutdown();
            try {
                if (!requestExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    requestExecutorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                requestExecutorService.shutdownNow();
                Thread.currentThread().interrupt();
            } finally {
                group.shutdownGracefully().sync();
            }
        }
    }

    private static TestkitRequestProcessorHandler.BackendMode getBackendMode(String[] args) {
        TestkitRequestProcessorHandler.BackendMode backendMode;
        var modeArg = args.length > 0 ? args[0] : null;
        if ("async".equals(modeArg)) {
            backendMode = TestkitRequestProcessorHandler.BackendMode.ASYNC;
        } else if ("reactive".equals(modeArg)) {
            backendMode = TestkitRequestProcessorHandler.BackendMode.REACTIVE;
        } else if ("reactive-streams".equals(modeArg)) {
            backendMode = TestkitRequestProcessorHandler.BackendMode.REACTIVE;
        } else {
            backendMode = TestkitRequestProcessorHandler.BackendMode.SYNC;
        }
        return backendMode;
    }
}
