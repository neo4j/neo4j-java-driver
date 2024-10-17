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

import static io.netty.buffer.ByteBufUtil.hexDump;
import static java.util.Objects.requireNonNull;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import java.util.Set;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.bolt.basicimpl.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltPatchesListener;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;

public class InboundMessageHandler extends SimpleChannelInboundHandler<ByteBuf> implements BoltPatchesListener {
    private final ByteBufInput input;
    private final MessageFormat messageFormat;
    private final LoggingProvider logging;

    private InboundMessageDispatcher messageDispatcher;
    private MessageFormat.Reader reader;
    private System.Logger log;

    public InboundMessageHandler(MessageFormat messageFormat, LoggingProvider logging) {
        this.input = new ByteBufInput();
        this.messageFormat = messageFormat;
        this.logging = logging;
        this.reader = messageFormat.newReader(input);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        var channel = ctx.channel();
        messageDispatcher = requireNonNull(ChannelAttributes.messageDispatcher(channel));
        log = new ChannelActivityLogger(channel, logging, getClass());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        messageDispatcher = null;
        log = null;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        if (messageDispatcher.fatalErrorOccurred()) {
            log.log(
                    System.Logger.Level.WARNING,
                    "Message ignored because of the previous fatal error. Channel will be closed. Message:\n%s",
                    hexDump(msg));
            return;
        }

        if (log.isLoggable(System.Logger.Level.TRACE)) {
            log.log(System.Logger.Level.TRACE, "S: %s", hexDump(msg));
        }

        input.start(msg);
        try {
            reader.read(messageDispatcher);
        } catch (Throwable error) {
            throw new DecoderException("Failed to read inbound message:\n" + hexDump(msg) + "\n", error);
        } finally {
            input.stop();
        }
    }

    @Override
    public void handle(Set<String> patches) {
        if (patches.contains(DATE_TIME_UTC_PATCH)) {
            messageFormat.enableDateTimeUtc();
            reader = messageFormat.newReader(input);
        }
    }
}
