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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TestkitMessageInboundHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final StringBuilder requestBuffer = new StringBuilder();

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        var requestStr = byteBuf.toString(CharsetUtil.UTF_8);
        requestBuffer.append(requestStr);

        List<String> testkitMessages = new ArrayList<>();
        var testkitMessageOpt = extractTestkitMessage();
        while (testkitMessageOpt.isPresent()) {
            testkitMessages.add(testkitMessageOpt.get());
            testkitMessageOpt = extractTestkitMessage();
        }

        testkitMessages.forEach(ctx::fireChannelRead);
    }

    private Optional<String> extractTestkitMessage() {
        var requestEndMarker = "#request end\n";
        var endMarkerIndex = requestBuffer.indexOf(requestEndMarker);
        if (endMarkerIndex < 0) {
            return Optional.empty();
        }
        var requestBeginMarker = "#request begin\n";
        var beginMarkerIndex = requestBuffer.indexOf(requestBeginMarker);
        if (beginMarkerIndex != 0) {
            throw new RuntimeException("Unexpected data in message buffer");
        }
        // extract Testkit message without markers
        var testkitMessage = requestBuffer.substring(requestBeginMarker.length(), endMarkerIndex);
        if (testkitMessage.contains(requestBeginMarker) || testkitMessage.contains(requestEndMarker)) {
            throw new RuntimeException("Testkit message contains request markers");
        }
        // remove Testkit message from buffer
        requestBuffer.delete(0, endMarkerIndex + requestEndMarker.length() + 1);
        return Optional.of(testkitMessage);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
