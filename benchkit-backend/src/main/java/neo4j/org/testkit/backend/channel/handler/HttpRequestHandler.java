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
package neo4j.org.testkit.backend.channel.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import neo4j.org.testkit.backend.handler.ReadyHandler;
import neo4j.org.testkit.backend.handler.WorkloadHandler;
import neo4j.org.testkit.backend.request.WorkloadRequest;

public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final System.Logger LOGGER = System.getLogger(WorkloadHandler.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final WorkloadHandler workloadHandler;
    private final ReadyHandler readyHandler;

    public HttpRequestHandler(WorkloadHandler workloadHandler, ReadyHandler readyHandler) {
        this.workloadHandler = Objects.requireNonNull(workloadHandler);
        this.readyHandler = Objects.requireNonNull(readyHandler);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (HttpUtil.is100ContinueExpected(request)) {
            send100Continue(ctx);
        }

        CompletionStage<FullHttpResponse> responseStage;
        if ("/workload".equals(request.uri()) && HttpMethod.PUT.equals(request.method())) {
            var content = request.content().toString(StandardCharsets.UTF_8);
            responseStage = CompletableFuture.completedStage(null)
                    .thenApply(ignored -> {
                        try {
                            return objectMapper.readValue(content, WorkloadRequest.class);
                        } catch (JsonProcessingException e) {
                            throw new CompletionException(e);
                        }
                    })
                    .thenCompose(workloadRequest -> workloadHandler.handle(request.protocolVersion(), workloadRequest));
        } else if ("/ready".equals(request.uri()) && HttpMethod.GET.equals(request.method())) {
            responseStage = readyHandler.ready(request.protocolVersion());
        } else {
            LOGGER.log(
                    System.Logger.Level.WARNING, "Unknown request %s with %s method.", request.uri(), request.method());
            responseStage = CompletableFuture.completedFuture(
                    new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
        }

        responseStage.whenComplete((response, throwable) -> {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        });
    }

    private static void send100Continue(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.log(System.Logger.Level.ERROR, "An unexpected error occured.", cause);
        ctx.close();
    }
}
