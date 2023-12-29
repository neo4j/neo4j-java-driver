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
package org.neo4j.driver.internal.bolt.basicimpl.async;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes.poolId;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import java.time.Clock;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.TerminationAwareStateLockingExecutor;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.ConnectionReadTimeoutHandler;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.ResetResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.Message;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.CommitMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.ResetMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.bolt.basicimpl.spi.Connection;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;
import org.neo4j.driver.internal.metrics.ListenerEvent;
import org.neo4j.driver.internal.metrics.MetricsListener;

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
    private final boolean telemetryEnabled;
    private final BoltProtocol protocol;
    private final CompletableFuture<Void> releaseFuture;
    private final Clock clock;
    private final MetricsListener metricsListener;
    private final ListenerEvent<?> inUseEvent;

    private final Long connectionReadTimeout;

    private Status status = Status.OPEN;
    private TerminationAwareStateLockingExecutor terminationAwareStateLockingExecutor;
    private ChannelHandler connectionReadTimeoutHandler;

    private DatabaseName databaseName;
    private AccessMode mode;
    private String impersonatedUser;

    public NetworkConnection(
            Channel channel,
            Clock clock,
            MetricsListener metricsListener,
            Logging logging,
            DatabaseName databaseName,
            AccessMode mode,
            String impersonatedUser) {
        this.log = logging.getLog(getClass());
        this.lock = new ReentrantLock();
        this.channel = channel;
        this.messageDispatcher = ChannelAttributes.messageDispatcher(channel);
        this.serverAgent = ChannelAttributes.serverAgent(channel);
        this.serverAddress = ChannelAttributes.serverAddress(channel);
        this.telemetryEnabled = ChannelAttributes.telemetryEnabled(channel);
        this.protocol = BoltProtocol.forChannel(channel);
        this.releaseFuture = new CompletableFuture<>();
        this.clock = clock;
        this.metricsListener = metricsListener;
        this.inUseEvent = metricsListener.createListenerEvent();
        this.connectionReadTimeout =
                ChannelAttributes.connectionReadTimeout(channel).orElse(null);
        this.databaseName = databaseName;
        this.mode = mode;
        this.impersonatedUser = impersonatedUser;
        metricsListener.afterConnectionCreated(poolId(this.channel), this.inUseEvent);
    }

    @Override
    public boolean isOpen() {
        return executeWithLock(lock, () -> status == Status.OPEN && channel.isOpen());
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
    public CompletionStage<Void> write(Message message, ResponseHandler handler) {
        return verifyOpen().thenCompose(ignored -> writeMessageInEventLoop(message, handler));
    }

    @Override
    public CompletionStage<Void> flush() {
        var future = new CompletableFuture<Void>();
        channel.eventLoop().execute(() -> {
            channel.flush();
            future.complete(null);
        });
        return future;
    }

    @Override
    public boolean isTelemetryEnabled() {
        return telemetryEnabled;
    }

    @Override
    public CompletionStage<Void> reset(Throwable throwable) {
        var result = new CompletableFuture<Void>();
        var handler = new ResetResponseHandler(messageDispatcher, result, throwable);
        writeResetMessageIfNeeded(handler, true);
        return result;
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
    public CompletionStage<Void> close() {
        var closeFuture = new CompletableFuture<Void>();
        writeMessageInEventLoop(GoodbyeMessage.GOODBYE, new NoOpResponseHandler())
                .thenCompose(ignored -> flush())
                .whenComplete((ignored, throwable) -> {
                    if (throwable == null) {
                        this.channel.close().addListener((ChannelFutureListener) future -> {
                            if (future.isSuccess()) {
                                closeFuture.complete(null);
                            } else {
                                closeFuture.completeExceptionally(future.cause());
                            }
                        });
                    } else {
                        closeFuture.completeExceptionally(throwable);
                    }
                });
        return closeFuture;
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

    @Override
    public AccessMode mode() {
        return mode;
    }

    @Override
    public void setDatabase(String database) {
        this.databaseName = DatabaseNameUtil.database(database);
    }

    @Override
    public void setAccessMode(AccessMode mode) {
        this.mode = mode;
    }

    @Override
    public void setImpersonatedUser(String impersonatedUser) {
        this.impersonatedUser = impersonatedUser;
    }

    @Override
    public DatabaseName databaseName() {
        return this.databaseName;
    }

    @Override
    public String impersonatedUser() {
        return impersonatedUser;
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

    private CompletionStage<Void> writeMessageInEventLoop(Message message, ResponseHandler handler) {
        var future = new CompletableFuture<Void>();
        channel.eventLoop()
                .execute(() -> terminationAwareStateLockingExecutor(message).execute(causeOfTermination -> {
                    if (causeOfTermination == null) {
                        if (messageDispatcher.fatalErrorOccurred() && GoodbyeMessage.GOODBYE.equals(message)) {
                            future.complete(null);
                            handler.onSuccess(Collections.emptyMap());
                            channel.close();
                            return;
                        }
                        messageDispatcher.enqueue(handler);
                        channel.write(message).addListener(writeFuture -> {
                            if (writeFuture.isSuccess()) {
                                registerConnectionReadTimeout(channel);
                            } else {
                                future.completeExceptionally(writeFuture.cause());
                            }
                        });
                        future.complete(null);
                    } else {
                        future.completeExceptionally(causeOfTermination);
                    }
                }));
        return future;
    }

    private void setAutoRead(boolean value) {
        channel.config().setAutoRead(value);
    }

    private CompletableFuture<Void> verifyOpen() {
        var connectionStatus = executeWithLock(lock, () -> status);
        var future = new CompletableFuture<Void>();
        switch (connectionStatus) {
            case RELEASED -> {
                Exception error =
                        new IllegalStateException("Connection has been released to the pool and can't be used");
                future.completeExceptionally(error);
            }
            case TERMINATED -> {
                Exception terminatedError =
                        new IllegalStateException("Connection has been terminated and can't be used");
                future.completeExceptionally(terminatedError);
            }
            default -> future.complete(null);
        }
        return future;
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
