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
package neo4j.org.testkit.backend.channel.handler;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import neo4j.org.testkit.backend.messages.TestkitModule;
import neo4j.org.testkit.backend.messages.requests.TestkitRequest;
import neo4j.org.testkit.backend.messages.responses.BackendError;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

public class TestkitRequestResponseMapperHandler extends ChannelDuplexHandler {
    private final ObjectMapper objectMapper = newObjectMapper();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String testkitMessage = (String) msg;
        TestkitRequest testkitRequest;
        testkitRequest = objectMapper.readValue(testkitMessage, TestkitRequest.class);
        ctx.fireChannelRead(testkitRequest);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        TestkitResponse testkitResponse = (TestkitResponse) msg;
        String responseStr = objectMapper.writeValueAsString(testkitResponse);
        ctx.writeAndFlush(responseStr, promise);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        TestkitResponse response = BackendError.builder()
                .data(BackendError.BackendErrorBody.builder()
                        .msg(cause.toString())
                        .build())
                .build();
        ctx.writeAndFlush(objectMapper.writeValueAsString(response));
    }

    public static ObjectMapper newObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        TestkitModule testkitModule = new TestkitModule();
        objectMapper.registerModule(testkitModule);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return objectMapper;
    }
}
