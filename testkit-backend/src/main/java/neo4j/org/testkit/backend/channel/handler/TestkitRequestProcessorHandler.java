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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.time.zone.ZoneRulesException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import neo4j.org.testkit.backend.CustomDriverError;
import neo4j.org.testkit.backend.FrontendError;
import neo4j.org.testkit.backend.ResponseQueueHanlder;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.TestkitRequest;
import neo4j.org.testkit.backend.messages.responses.BackendError;
import neo4j.org.testkit.backend.messages.responses.DriverError;
import neo4j.org.testkit.backend.messages.responses.GqlError;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.RetryableException;
import org.neo4j.driver.exceptions.UntrustedServerException;

public class TestkitRequestProcessorHandler extends ChannelInboundHandlerAdapter {
    private final TestkitState testkitState;
    private final BiFunction<TestkitRequest, TestkitState, CompletionStage<TestkitResponse>> processorImpl;
    // Some requests require multiple threads
    private final Executor requestExecutorService = Executors.newFixedThreadPool(10);
    private final ResponseQueueHanlder responseQueueHanlder;
    private Channel channel;

    public TestkitRequestProcessorHandler(BackendMode backendMode, ResponseQueueHanlder responseQueueHanlder) {
        switch (backendMode) {
            case ASYNC -> processorImpl = TestkitRequest::processAsync;
            case REACTIVE -> processorImpl =
                    (request, state) -> request.processReactive(state).toFuture();
            default -> processorImpl = TestkitRequestProcessorHandler::wrapSyncRequest;
        }
        testkitState = new TestkitState(this::writeAndFlush);
        this.responseQueueHanlder = responseQueueHanlder;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        channel = ctx.channel();
        super.channelRegistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Processing is done in a separate thread to avoid blocking EventLoop because some testing logic, like
        // resolvers support, is blocking.
        requestExecutorService.execute(() -> {
            try {
                var request = (TestkitRequest) msg;
                processorImpl
                        .apply(request, testkitState)
                        .exceptionally(this::createErrorResponse)
                        .whenComplete((response, ignored) -> {
                            if (response != null) {
                                responseQueueHanlder.offerAndDispatchFirstResponse(response);
                            }
                        });
            } catch (Throwable throwable) {
                exceptionCaught(ctx, throwable);
            }
        });
    }

    private static CompletionStage<TestkitResponse> wrapSyncRequest(
            TestkitRequest testkitRequest, TestkitState testkitState) {
        var result = new CompletableFuture<TestkitResponse>();
        try {
            result.complete(testkitRequest.process(testkitState));
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
        return result;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        var response = createErrorResponse(cause);
        responseQueueHanlder.offerAndDispatchFirstResponse(response);
    }

    private TestkitResponse createErrorResponse(Throwable throwable) {
        if (throwable instanceof CompletionException) {
            throwable = throwable.getCause();
        }
        if (throwable instanceof Neo4jException e) {
            var id = testkitState.newId();
            testkitState.getErrors().put(id, e);
            return mapToDriverError(id, e);
        } else if (isConnectionPoolClosedException(throwable)
                || throwable instanceof UntrustedServerException
                || throwable instanceof NoSuchRecordException
                || throwable instanceof ZoneRulesException) {
            var id = testkitState.newId();
            testkitState.getErrors().put(id, (Exception) throwable);
            return DriverError.builder()
                    .data(DriverError.DriverErrorBody.builder()
                            .id(id)
                            .errorType(throwable.getClass().getName())
                            .msg(throwable.getMessage())
                            .build())
                    .build();
        } else if (throwable instanceof CustomDriverError) {
            throwable = throwable.getCause();
            var id = testkitState.newId();
            return DriverError.builder()
                    .data(DriverError.DriverErrorBody.builder()
                            .id(id)
                            .errorType(throwable.getClass().getName())
                            .msg(throwable.getMessage())
                            .build())
                    .build();
        } else if (throwable instanceof FrontendError) {
            return neo4j.org.testkit.backend.messages.responses.FrontendError.builder()
                    .build();
        } else {
            return BackendError.builder()
                    .data(BackendError.BackendErrorBody.builder()
                            .msg(throwable.toString())
                            .build())
                    .build();
        }
    }

    private DriverError mapToDriverError(String id, Neo4jException e) {
        return DriverError.builder()
                .data(DriverError.DriverErrorBody.builder()
                        .id(id)
                        .errorType(e.getClass().getName())
                        .gqlStatus(e.gqlStatus())
                        .statusDescription(e.statusDescription())
                        .code(e.code())
                        .msg(e.getMessage())
                        .diagnosticRecord(e.diagnosticRecord())
                        .classification(e.classification().map(Enum::toString).orElse("UNKNOWN"))
                        .rawClassification(e.rawClassification().orElse(null))
                        .retryable(e instanceof RetryableException)
                        .cause(mapToGqlError(e.gqlCause().orElse(null)))
                        .build())
                .build();
    }

    private GqlError mapToGqlError(Neo4jException e) {
        if (e == null) {
            return null;
        }
        return GqlError.builder()
                .data(GqlError.GqlErrorBody.builder()
                        .gqlStatus(e.gqlStatus())
                        .statusDescription(e.statusDescription())
                        .msg(e.getMessage())
                        .diagnosticRecord(e.diagnosticRecord())
                        .classification(e.classification().map(Enum::toString).orElse("UNKNOWN"))
                        .rawClassification(e.rawClassification().orElse(null))
                        .cause(mapToGqlError(e.gqlCause().orElse(null)))
                        .build())
                .build();
    }

    private boolean isConnectionPoolClosedException(Throwable throwable) {
        return throwable instanceof IllegalStateException
                && throwable.getMessage() != null
                && throwable.getMessage().equals("Connection provider is closed.");
    }

    private void writeAndFlush(TestkitResponse response) {
        if (channel == null) {
            throw new IllegalStateException("Called before channel is initialized");
        }
        responseQueueHanlder.offerAndDispatchFirstResponse(response);
    }

    public enum BackendMode {
        SYNC,
        ASYNC,
        REACTIVE
    }
}
