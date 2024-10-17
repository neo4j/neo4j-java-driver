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
package org.neo4j.driver.internal.bolt.basicimpl.async.outbound;

import static io.netty.buffer.ByteBufUtil.hexDump;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;
import java.util.Set;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.BoltProtocolUtil;
import org.neo4j.driver.internal.bolt.basicimpl.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltPatchesListener;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;

public class OutboundMessageHandler extends MessageToMessageEncoder<Message> implements BoltPatchesListener {
    public static final String NAME = OutboundMessageHandler.class.getSimpleName();
    private final ChunkAwareByteBufOutput output;
    private final MessageFormat messageFormat;
    private final LoggingProvider logging;

    private MessageFormat.Writer writer;
    private System.Logger log;

    public OutboundMessageHandler(MessageFormat messageFormat, LoggingProvider logging) {
        this.output = new ChunkAwareByteBufOutput();
        this.messageFormat = messageFormat;
        this.logging = logging;
        this.writer = messageFormat.newWriter(output);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        log = new ChannelActivityLogger(ctx.channel(), logging, getClass());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        log = null;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) {
        log.log(System.Logger.Level.DEBUG, "C: %s", msg);

        var messageBuf = ctx.alloc().ioBuffer();
        output.start(messageBuf);
        try {
            writer.write(msg);
            output.stop();
        } catch (Throwable error) {
            output.stop();
            // release buffer because it will not get added to the out list and no other handler is going to handle it
            messageBuf.release();
            throw new EncoderException("Failed to write outbound message: " + msg, error);
        }

        if (log.isLoggable(System.Logger.Level.TRACE)) {
            log.log(System.Logger.Level.TRACE, "C: %s", hexDump(messageBuf));
        }

        BoltProtocolUtil.writeMessageBoundary(messageBuf);
        out.add(messageBuf);
    }

    @Override
    public void handle(Set<String> patches) {
        if (patches.contains(DATE_TIME_UTC_PATCH)) {
            messageFormat.enableDateTimeUtc();
            writer = messageFormat.newWriter(output);
        }
    }
}
