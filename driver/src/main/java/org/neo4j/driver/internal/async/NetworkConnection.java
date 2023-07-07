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
package org.neo4j.driver.internal.async;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.poolId;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setTerminationReason;
import static org.neo4j.driver.internal.util.Futures.asCompletionStage;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.async.inbound.ConnectionReadTimeoutHandler;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.pool.ExtendedChannelPool;
import org.neo4j.driver.internal.handlers.ChannelReleasingResetResponseHandler;
import org.neo4j.driver.internal.handlers.ResetResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.request.CommitMessage;
import org.neo4j.driver.internal.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.ResetMessage;
import org.neo4j.driver.internal.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.metrics.ListenerEvent;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;

/**
 * This connection represents a simple network connection to a remote server. It wraps a channel obtained from a connection pool. The life cycle of this
 * connection start from the moment the channel is borrowed out of the pool and end at the time the connection is released back to the pool.
 */
public class NetworkConnection implements Connection {
    private final Logger log;
    private final Lock lock;
    private final Channel channel;
    private final InboundMessageDispatcher messageDispatcher;
    private final String serverAgent;
    private final BoltServerAddress serverAddress;
    private final BoltProtocol protocol;
    private final ExtendedChannelPool channelPool;
    private final CompletableFuture<Void> releaseFuture;
    private final Clock clock;
    private final MetricsListener metricsListener;
    private final ListenerEvent<?> inUseEvent;

    private final Long connectionReadTimeout;

    private Status status = Status.OPEN;
    private TerminationAwareStateLockingExecutor terminationAwareStateLockingExecutor;
    private ChannelHandler connectionReadTimeoutHandler;

    public NetworkConnection(
            Channel channel,
            ExtendedChannelPool channelPool,
            Clock clock,
            MetricsListener metricsListener,
            Logging logging) {
        this.log = logging.getLog(getClass());
        this.lock = new ReentrantLock();
        this.channel = channel;
        this.messageDispatcher = ChannelAttributes.messageDispatcher(channel);
        this.serverAgent = ChannelAttributes.serverAgent(channel);
        this.serverAddress = ChannelAttributes.serverAddress(channel);
        this.protocol = BoltProtocol.forChannel(channel);
        this.channelPool = channelPool;
        this.releaseFuture = new CompletableFuture<>();
        this.clock = clock;
        this.metricsListener = metricsListener;
        this.inUseEvent = metricsListener.createListenerEvent();
        this.connectionReadTimeout =
                ChannelAttributes.connectionReadTimeout(channel).orElse(null);
        metricsListener.afterConnectionCreated(poolId(this.channel), this.inUseEvent);
    }

    @Override
    public boolean isOpen() {
        return executeWithLock(lock, () -> status == Status.OPEN);
    }

    @Override
    public void enableAutoRead() {
        if (isOpen()) {
            setAutoRead(true);
        }
    }

    @Override
    public void disableAutoRead() {
        if (isOpen()) {
            setAutoRead(false);
        }
    }

    @Override
    public void write(Message message, ResponseHandler handler) {
        if (verifyOpen(handler)) {
            writeMessageInEventLoop(message, handler, false);
        }
    }

    @Override
    public void writeAndFlush(Message message, ResponseHandler handler) {
        if (verifyOpen(handler)) {
            writeMessageInEventLoop(message, handler, true);
        }
    }

    @Override
    public CompletionStage<Void> reset(Throwable throwable) {
        var result = new CompletableFuture<Void>();
        var handler = new ResetResponseHandler(messageDispatcher, result, throwable);
        writeResetMessageIfNeeded(handler, true);
        return result;
    }

    @Override
    public CompletionStage<Void> release() {
        if (executeWithLock(lock, () -> updateStateIfOpen(Status.RELEASED))) {
            var handler = new ChannelReleasingResetResponseHandler(
                    channel, channelPool, messageDispatcher, clock, releaseFuture);

            writeResetMessageIfNeeded(handler, false);
            metricsListener.afterConnectionReleased(poolId(this.channel), this.inUseEvent);
        }
        return releaseFuture;
    }

    @Override
    public void terminateAndRelease(String reason) {
        if (executeWithLock(lock, () -> updateStateIfOpen(Status.TERMINATED))) {
            setTerminationReason(channel, reason);
            asCompletionStage(channel.close())
                    .exceptionally(throwable -> null)
                    .thenCompose(ignored -> channelPool.release(channel))
                    .whenComplete((ignored, throwable) -> {
                        releaseFuture.complete(null);
                        metricsListener.afterConnectionReleased(poolId(this.channel), this.inUseEvent);
                    });
        }
    }

    @Override
    public String serverAgent() {
        return serverAgent;
    }

    @Override
    public BoltServerAddress serverAddress() {
        return serverAddress;
    }

    @Override
    public BoltProtocol protocol() {
        return protocol;
    }

    @Override
    public void bindTerminationAwareStateLockingExecutor(TerminationAwareStateLockingExecutor executor) {
        executeWithLock(lock, () -> {
            if (this.terminationAwareStateLockingExecutor != null) {
                throw new IllegalStateException("terminationAwareStateLockingExecutor is already set");
            }
            this.terminationAwareStateLockingExecutor = executor;
        });
    }

    private boolean updateStateIfOpen(Status newStatus) {
        if (Status.OPEN.equals(status)) {
            status = newStatus;
            return true;
        } else {
            return false;
        }
    }

    private void writeResetMessageIfNeeded(ResponseHandler resetHandler, boolean isSessionReset) {
        channel.eventLoop().execute(() -> {
            if (isSessionReset && !isOpen()) {
                resetHandler.onSuccess(emptyMap());
            } else {
                // auto-read could've been disabled, re-enable it to automatically receive response for RESET
                setAutoRead(true);

                messageDispatcher.enqueue(resetHandler);
                channel.writeAndFlush(ResetMessage.RESET).addListener(future -> registerConnectionReadTimeout(channel));
            }
        });
    }

    private void writeMessageInEventLoop(Message message, ResponseHandler handler, boolean flush) {
        channel.eventLoop()
                .execute(() -> terminationAwareStateLockingExecutor(message).execute(causeOfTermination -> {
                    if (causeOfTermination == null) {
                        messageDispatcher.enqueue(handler);

                        if (flush) {
                            channel.writeAndFlush(message)
                                    .addListener(future -> registerConnectionReadTimeout(channel));
                        } else {
                            channel.write(message, channel.voidPromise());
                        }
                    } else {
                        handler.onFailure(causeOfTermination);
                    }
                }));
    }

    private void setAutoRead(boolean value) {
        channel.config().setAutoRead(value);
    }

    private boolean verifyOpen(ResponseHandler handler) {
        var connectionStatus = executeWithLock(lock, () -> status);
        return switch (connectionStatus) {
            case OPEN -> true;
            case RELEASED -> {
                Exception error =
                        new IllegalStateException("Connection has been released to the pool and can't be used");
                if (handler != null) {
                    handler.onFailure(error);
                }
                yield false;
            }
            case TERMINATED -> {
                Exception terminatedError =
                        new IllegalStateException("Connection has been terminated and can't be used");
                if (handler != null) {
                    handler.onFailure(terminatedError);
                }
                yield false;
            }
        };
    }

    private void registerConnectionReadTimeout(Channel channel) {
        if (!channel.eventLoop().inEventLoop()) {
            throw new IllegalStateException("This method may only be called in the EventLoop");
        }

        if (connectionReadTimeout != null && connectionReadTimeoutHandler == null) {
            connectionReadTimeoutHandler = new ConnectionReadTimeoutHandler(connectionReadTimeout, TimeUnit.SECONDS);
            channel.pipeline().addFirst(connectionReadTimeoutHandler);
            log.debug("Added ConnectionReadTimeoutHandler");
            messageDispatcher.setBeforeLastHandlerHook((messageType) -> {
                channel.pipeline().remove(connectionReadTimeoutHandler);
                connectionReadTimeoutHandler = null;
                messageDispatcher.setBeforeLastHandlerHook(null);
                log.debug("Removed ConnectionReadTimeoutHandler");
            });
        }
    }

    private TerminationAwareStateLockingExecutor terminationAwareStateLockingExecutor(Message message) {
        var result = (TerminationAwareStateLockingExecutor) consumer -> consumer.accept(null);
        if (isQueryMessage(message)) {
            var lockingExecutor = executeWithLock(lock, () -> this.terminationAwareStateLockingExecutor);
            if (lockingExecutor != null) {
                result = lockingExecutor;
            }
        }
        return result;
    }

    private boolean isQueryMessage(Message message) {
        return message instanceof RunWithMetadataMessage
                || message instanceof PullMessage
                || message instanceof PullAllMessage
                || message instanceof DiscardMessage
                || message instanceof DiscardAllMessage
                || message instanceof CommitMessage
                || message instanceof RollbackMessage;
    }

    private enum Status {
        OPEN,
        RELEASED,
        TERMINATED
    }
}
