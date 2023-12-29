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

import static java.util.Objects.requireNonNull;

import io.netty.channel.Channel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.exception.MessageIgnoredException;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.ResetResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;
import org.neo4j.driver.internal.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.logging.ChannelErrorLogger;
import org.neo4j.driver.internal.util.ErrorUtil;

public class InboundMessageDispatcher implements ResponseMessageHandler {
    private final Channel channel;
    private final Queue<ResponseHandler> handlers = new LinkedList<>();
    private final Logger log;
    private final ChannelErrorLogger errorLog;

    private volatile boolean gracefullyClosed;
    private boolean fatalErrorOccurred;
    private HandlerHook beforeLastHandlerHook;

    private ResponseHandler autoReadManagingHandler;

    public InboundMessageDispatcher(Channel channel, Logging logging) {
        this.channel = requireNonNull(channel);
        this.log = new ChannelActivityLogger(channel, logging, getClass());
        this.errorLog = new ChannelErrorLogger(channel, logging);
    }

    public void enqueue(ResponseHandler handler) {
        if (fatalErrorOccurred) {
            log.info(String.format("No handlers are accepted %s", handler.toString()));
            handler.onFailure(new IllegalStateException("No handlers are accepted after fatal error"));
        } else {
            handlers.add(handler);
            updateAutoReadManagingHandlerIfNeeded(handler);
        }
    }

    public void setBeforeLastHandlerHook(HandlerHook beforeLastHandlerHook) {
        if (!channel.eventLoop().inEventLoop()) {
            throw new IllegalStateException("This method may only be called in the EventLoop");
        }
        this.beforeLastHandlerHook = beforeLastHandlerHook;
    }

    public int queuedHandlersCount() {
        return handlers.size();
    }

    @Override
    public void handleSuccessMessage(Map<String, Value> meta) {
        log.debug("S: SUCCESS %s", meta);
        invokeBeforeLastHandlerHook(HandlerHook.MessageType.SUCCESS);
        var handler = removeHandler();
        handler.onSuccess(meta);
    }

    @Override
    public void handleRecordMessage(Value[] fields) {
        if (log.isDebugEnabled()) {
            log.debug("S: RECORD %s", Arrays.toString(fields));
        }
        var handler = handlers.peek();
        if (handler == null) {
            throw new IllegalStateException(
                    "No handler exists to handle RECORD message with fields: " + Arrays.toString(fields));
        }
        handler.onRecord(fields);
    }

    @Override
    public void handleFailureMessage(String code, String message) {
        log.debug("S: FAILURE %s \"%s\"", code, message);

        var error = ErrorUtil.newNeo4jError(code, message);
        invokeBeforeLastHandlerHook(HandlerHook.MessageType.FAILURE);
        var handler = removeHandler();
        handler.onFailure(error);
    }

    @Override
    public void handleIgnoredMessage() {
        log.debug("S: IGNORED");

        var handler = removeHandler();
        handler.onFailure(new MessageIgnoredException("The server has ignored the message"));
    }

    public HandlerHook getBeforeLastHandlerHook() {
        return this.beforeLastHandlerHook;
    }

    private Optional<ResetResponseHandler> getPendingResetHandler() {
        return handlers.stream()
                .filter(h -> h instanceof ResetResponseHandler)
                .map(h -> (ResetResponseHandler) h)
                .findFirst();
    }

    public void handleChannelInactive(Throwable cause) {
        // report issue if the connection has not been terminated as a result of a graceful shutdown request from its
        // parent pool
        if (!gracefullyClosed) {
            handleChannelError(cause);
        } else {
            while (!handlers.isEmpty()) {
                var handler = removeHandler();
                handler.onFailure(cause);
            }
            channel.close();
        }
    }

    public void handleChannelError(Throwable error) {
        this.fatalErrorOccurred = true;

        while (!this.handlers.isEmpty()) {
            var handler = removeHandler();
            handler.onFailure(error);
        }

        this.channel.close();
    }

    public boolean fatalErrorOccurred() {
        return fatalErrorOccurred;
    }

    public void prepareToCloseChannel() {
        this.gracefullyClosed = true;
    }

    /**
     * <b>Visible for testing</b>
     */
    ResponseHandler autoReadManagingHandler() {
        return autoReadManagingHandler;
    }

    private ResponseHandler removeHandler() {
        var handler = handlers.remove();
        if (handler == autoReadManagingHandler) {
            // the auto-read managing handler is being removed
            // make sure this dispatcher does not hold on to a removed handler
            updateAutoReadManagingHandler(null);
        }
        return handler;
    }

    private void updateAutoReadManagingHandlerIfNeeded(ResponseHandler handler) {
        if (handler.canManageAutoRead()) {
            updateAutoReadManagingHandler(handler);
        }
    }

    private void updateAutoReadManagingHandler(ResponseHandler newHandler) {
        if (autoReadManagingHandler != null) {
            // there already exists a handler that manages channel's auto-read
            // make it stop because new managing handler is being added and there should only be a single such handler
            autoReadManagingHandler.disableAutoReadManagement();
            // restore the default value of auto-read
            channel.config().setAutoRead(true);
        }
        autoReadManagingHandler = newHandler;
    }

    private void invokeBeforeLastHandlerHook(HandlerHook.MessageType messageType) {
        if (handlers.size() == 1 && beforeLastHandlerHook != null) {
            beforeLastHandlerHook.run(messageType);
        }
    }

    public interface HandlerHook {
        enum MessageType {
            SUCCESS,
            FAILURE
        }

        void run(MessageType messageType);
    }

    //    For testing only
    Logger getLog() {
        return log;
    }

    //    For testing only
    Logger getErrorLog() {
        return errorLog;
    }
}
