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
package org.neo4j.driver.internal.bolt.basicimpl.async.inbound;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder {
    private static final Cumulator DEFAULT_CUMULATOR = determineDefaultCumulator();

    private boolean readMessageBoundary;

    public MessageDecoder() {
        setCumulator(DEFAULT_CUMULATOR);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            // on every read check if input buffer is empty or not
            // if it is empty then it's a message boundary and full message is in the buffer
            readMessageBoundary = ((ByteBuf) msg).readableBytes() == 0;
        }
        super.channelRead(ctx, msg);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (readMessageBoundary) {
            // now we have a complete message in the input buffer

            // increment ref count of the buffer and create it's duplicate that shares the content
            // duplicate will be the output of this decoded and input for the next one
            var messageBuf = in.retainedDuplicate();

            // signal that whole message was read by making input buffer seem like it was fully read/consumed
            in.readerIndex(in.readableBytes());

            // pass the full message to the next handler in the pipeline
            out.add(messageBuf);

            readMessageBoundary = false;
        }
    }

    private static Cumulator determineDefaultCumulator() {
        var value = System.getProperty("messageDecoderCumulator", "");
        if ("merge".equals(value)) {
            return MERGE_CUMULATOR;
        }
        return COMPOSITE_CUMULATOR;
    }
}
