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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.terminationReason;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.messaging.request.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.messaging.request.ResetMessage.RESET;
import static org.neo4j.driver.internal.util.Iterables.single;
import static org.neo4j.driver.testutil.DaemonThreadFactory.daemon;
import static org.neo4j.driver.testutil.TestUtil.DEFAULT_TEST_PROTOCOL_VERSION;

import io.netty.channel.Channel;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.neo4j.driver.Query;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.async.pool.ExtendedChannelPool;
import org.neo4j.driver.internal.handlers.NoOpResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.request.CommitMessage;
import org.neo4j.driver.internal.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.messaging.request.PullAllMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.metrics.DevNullMetricsListener;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.FakeClock;

class NetworkConnectionTest {
    private static final NoOpResponseHandler NO_OP_HANDLER = NoOpResponseHandler.INSTANCE;

    private ExecutorService executor;
    private EventLoop eventLoop;

    @AfterEach
    void tearDown() throws Exception {
        shutdownEventLoop();
    }

    @Test
    void shouldBeOpenAfterCreated() {
        var connection = newConnection(newChannel());
        assertTrue(connection.isOpen());
    }

    @Test
    void shouldNotBeOpenAfterRelease() {
        var connection = newConnection(newChannel());
        connection.release();
        assertFalse(connection.isOpen());
    }

    @Test
    void shouldSendResetOnRelease() {
        var channel = newChannel();
        var connection = newConnection(channel);

        connection.release();
        channel.runPendingTasks();

        assertEquals(1, channel.outboundMessages().size());
        assertEquals(RESET, channel.readOutbound());
    }

    @Test
    void shouldWriteInEventLoopThread() throws Exception {
        testWriteInEventLoop(
                "WriteSingleMessage",
                connection -> connection.write(
                        RunWithMetadataMessage.unmanagedTxRunMessage(new Query("RETURN 1")), NO_OP_HANDLER));
    }

    @Test
    void shouldWriteAndFlushInEventLoopThread() throws Exception {
        testWriteInEventLoop(
                "WriteAndFlushSingleMessage",
                connection -> connection.writeAndFlush(
                        RunWithMetadataMessage.unmanagedTxRunMessage(new Query("RETURN 1")), NO_OP_HANDLER));
    }

    @Test
    void shouldWriteForceReleaseInEventLoopThread() throws Exception {
        testWriteInEventLoop("ReleaseTestEventLoop", NetworkConnection::release);
    }

    @Test
    void shouldEnableAutoReadWhenReleased() {
        var channel = newChannel();
        channel.config().setAutoRead(false);

        var connection = newConnection(channel);

        connection.release();
        channel.runPendingTasks();

        assertTrue(channel.config().isAutoRead());
    }

    @Test
    void shouldNotDisableAutoReadWhenReleased() {
        var channel = newChannel();
        channel.config().setAutoRead(true);

        var connection = newConnection(channel);

        connection.release();
        connection.disableAutoRead(); // does nothing on released connection
        assertTrue(channel.config().isAutoRead());
    }

    @Test
    void shouldWriteSingleMessage() {
        var channel = newChannel();
        var connection = newConnection(channel);

        connection.write(PULL_ALL, NO_OP_HANDLER);

        assertEquals(0, channel.outboundMessages().size());
        channel.flushOutbound();
        assertEquals(1, channel.outboundMessages().size());
        assertEquals(PULL_ALL, single(channel.outboundMessages()));
    }

    @Test
    void shouldWriteAndFlushSingleMessage() {
        var channel = newChannel();
        var connection = newConnection(channel);

        connection.writeAndFlush(PULL_ALL, NO_OP_HANDLER);
        channel.runPendingTasks(); // writeAndFlush is scheduled to execute in the event loop thread, trigger its
        // execution

        assertEquals(1, channel.outboundMessages().size());
        assertEquals(PULL_ALL, single(channel.outboundMessages()));
    }

    @Test
    void shouldNotWriteSingleMessageWhenReleased() {
        var handler = mock(ResponseHandler.class);
        var connection = newConnection(newChannel());

        connection.release();
        connection.write(RunWithMetadataMessage.unmanagedTxRunMessage(new Query("RETURN 1")), handler);

        var failureCaptor = ArgumentCaptor.forClass(IllegalStateException.class);
        verify(handler).onFailure(failureCaptor.capture());
        assertConnectionReleasedError(failureCaptor.getValue());
    }

    @Test
    void shouldNotWriteAndFlushSingleMessageWhenReleased() {
        var handler = mock(ResponseHandler.class);
        var connection = newConnection(newChannel());

        connection.release();
        connection.writeAndFlush(RunWithMetadataMessage.unmanagedTxRunMessage(new Query("RETURN 1")), handler);

        var failureCaptor = ArgumentCaptor.forClass(IllegalStateException.class);
        verify(handler).onFailure(failureCaptor.capture());
        assertConnectionReleasedError(failureCaptor.getValue());
    }

    @Test
    void shouldNotWriteSingleMessageWhenTerminated() {
        var handler = mock(ResponseHandler.class);
        var connection = newConnection(newChannel());

        connection.terminateAndRelease("42");
        connection.write(RunWithMetadataMessage.unmanagedTxRunMessage(new Query("RETURN 1")), handler);

        var failureCaptor = ArgumentCaptor.forClass(IllegalStateException.class);
        verify(handler).onFailure(failureCaptor.capture());
        assertConnectionTerminatedError(failureCaptor.getValue());
    }

    @Test
    void shouldNotWriteAndFlushSingleMessageWhenTerminated() {
        var handler = mock(ResponseHandler.class);
        var connection = newConnection(newChannel());

        connection.terminateAndRelease("42");
        connection.writeAndFlush(RunWithMetadataMessage.unmanagedTxRunMessage(new Query("RETURN 1")), handler);

        var failureCaptor = ArgumentCaptor.forClass(IllegalStateException.class);
        verify(handler).onFailure(failureCaptor.capture());
        assertConnectionTerminatedError(failureCaptor.getValue());
    }

    @Test
    void shouldReturnServerAgentWhenCreated() {
        var channel = newChannel();
        var agent = "Neo4j/4.2.5";
        ChannelAttributes.setServerAgent(channel, agent);

        var connection = newConnection(channel);

        assertEquals(agent, connection.serverAgent());
    }

    @Test
    void shouldReturnServerAgentWhenReleased() {
        var channel = newChannel();
        var agent = "Neo4j/4.2.5";
        ChannelAttributes.setServerAgent(channel, agent);

        var connection = newConnection(channel);
        connection.release();

        assertEquals(agent, connection.serverAgent());
    }

    @Test
    void shouldReturnServerAddressWhenReleased() {
        var channel = newChannel();
        var address = new BoltServerAddress("host", 4242);
        ChannelAttributes.setServerAddress(channel, address);

        var connection = newConnection(channel);
        connection.release();

        assertEquals(address, connection.serverAddress());
    }

    @Test
    void shouldReturnSameCompletionStageFromRelease() {
        var channel = newChannel();
        var connection = newConnection(channel);

        var releaseStage1 = connection.release();
        var releaseStage2 = connection.release();
        var releaseStage3 = connection.release();

        channel.runPendingTasks();

        // RESET should be send only once
        assertEquals(1, channel.outboundMessages().size());
        assertEquals(RESET, channel.outboundMessages().poll());

        // all returned stages should be the same
        assertEquals(releaseStage1, releaseStage2);
        assertEquals(releaseStage2, releaseStage3);
    }

    @Test
    void shouldEnableAutoRead() {
        var channel = newChannel();
        channel.config().setAutoRead(false);
        var connection = newConnection(channel);

        connection.enableAutoRead();

        assertTrue(channel.config().isAutoRead());
    }

    @Test
    void shouldDisableAutoRead() {
        var channel = newChannel();
        channel.config().setAutoRead(true);
        var connection = newConnection(channel);

        connection.disableAutoRead();

        assertFalse(channel.config().isAutoRead());
    }

    @Test
    void shouldSetTerminationReasonOnChannelWhenTerminated() {
        var channel = newChannel();
        var connection = newConnection(channel);

        var reason = "Something really bad has happened";
        connection.terminateAndRelease(reason);

        assertEquals(reason, terminationReason(channel));
    }

    @Test
    void shouldCloseChannelWhenTerminated() {
        var channel = newChannel();
        var connection = newConnection(channel);
        assertTrue(channel.isActive());

        connection.terminateAndRelease("test");

        assertFalse(channel.isActive());
    }

    @Test
    void shouldReleaseChannelWhenTerminated() {
        var channel = newChannel();
        var pool = mock(ExtendedChannelPool.class);
        var connection = newConnection(channel, pool);
        verify(pool, never()).release(any());

        connection.terminateAndRelease("test");

        verify(pool).release(channel);
    }

    @Test
    void shouldNotReleaseChannelMultipleTimesWhenTerminatedMultipleTimes() {
        var channel = newChannel();
        var pool = mock(ExtendedChannelPool.class);
        var connection = newConnection(channel, pool);
        verify(pool, never()).release(any());

        connection.terminateAndRelease("reason 1");
        connection.terminateAndRelease("reason 2");
        connection.terminateAndRelease("reason 3");

        // channel is terminated with the first termination reason
        assertEquals("reason 1", terminationReason(channel));
        // channel is released to the pool only once
        verify(pool).release(channel);
    }

    @Test
    void shouldNotReleaseAfterTermination() {
        var channel = newChannel();
        var pool = mock(ExtendedChannelPool.class);
        var connection = newConnection(channel, pool);
        verify(pool, never()).release(any());

        connection.terminateAndRelease("test");
        var releaseStage = connection.release();

        // release stage should be completed immediately
        assertTrue(releaseStage.toCompletableFuture().isDone());
        // channel is released to the pool only once
        verify(pool).release(channel);
    }

    @Test
    void shouldSendResetMessageWhenReset() {
        var channel = newChannel();
        var connection = newConnection(channel);

        connection.reset(null);
        channel.runPendingTasks();

        assertEquals(1, channel.outboundMessages().size());
        assertEquals(RESET, channel.readOutbound());
    }

    @Test
    void shouldCompleteResetFutureWhenSuccessResponseArrives() {
        var channel = newChannel();
        var connection = newConnection(channel);

        var resetFuture = connection.reset(null).toCompletableFuture();
        channel.runPendingTasks();
        assertFalse(resetFuture.isDone());

        messageDispatcher(channel).handleSuccessMessage(emptyMap());
        assertTrue(resetFuture.isDone());
        assertFalse(resetFuture.isCompletedExceptionally());
    }

    @Test
    void shouldCompleteResetFutureWhenFailureResponseArrives() {
        var channel = newChannel();
        var connection = newConnection(channel);

        var resetFuture = connection.reset(null).toCompletableFuture();
        channel.runPendingTasks();
        assertFalse(resetFuture.isDone());

        messageDispatcher(channel).handleFailureMessage("Neo.TransientError.Transaction.Terminated", "Message");
        assertTrue(resetFuture.isDone());
        assertFalse(resetFuture.isCompletedExceptionally());
    }

    @Test
    void shouldDoNothingInResetWhenClosed() {
        var channel = newChannel();
        var connection = newConnection(channel);

        connection.release();
        channel.runPendingTasks();

        var resetFuture = connection.reset(null).toCompletableFuture();
        channel.runPendingTasks();

        assertEquals(1, channel.outboundMessages().size());
        assertEquals(RESET, channel.readOutbound());
        assertTrue(resetFuture.isDone());
        assertFalse(resetFuture.isCompletedExceptionally());
    }

    @Test
    void shouldEnableAutoReadWhenDoingReset() {
        var channel = newChannel();
        channel.config().setAutoRead(false);
        var connection = newConnection(channel);

        connection.reset(null);
        channel.runPendingTasks();

        assertTrue(channel.config().isAutoRead());
    }

    @Test
    void shouldRejectBindingTerminationAwareStateLockingExecutorTwice() {
        var channel = newChannel();
        var connection = newConnection(channel);
        var lockingExecutor = mock(TerminationAwareStateLockingExecutor.class);
        connection.bindTerminationAwareStateLockingExecutor(lockingExecutor);

        assertThrows(
                IllegalStateException.class,
                () -> connection.bindTerminationAwareStateLockingExecutor(lockingExecutor));
    }

    @ParameterizedTest
    @MethodSource("queryMessages")
    void shouldPreventDispatchingQueryMessagesOnTermination(QueryMessage queryMessage) {
        // Given
        var channel = newChannel();
        var connection = newConnection(channel);
        var lockingExecutor = mock(TerminationAwareStateLockingExecutor.class);
        var error = mock(Neo4jException.class);
        doAnswer(invocationOnMock -> {
                    @SuppressWarnings("unchecked")
                    var consumer = (Consumer<Throwable>) invocationOnMock.getArguments()[0];
                    consumer.accept(error);
                    return null;
                })
                .when(lockingExecutor)
                .execute(any());
        connection.bindTerminationAwareStateLockingExecutor(lockingExecutor);
        var handler = mock(ResponseHandler.class);

        // When
        if (queryMessage.flush()) {
            connection.writeAndFlush(queryMessage.message(), handler);
        } else {
            connection.write(queryMessage.message(), handler);
        }
        channel.runPendingTasks();

        // Then
        assertTrue(channel.outboundMessages().isEmpty());
        then(lockingExecutor).should().execute(any());
        then(handler).should().onFailure(error);
    }

    @ParameterizedTest
    @MethodSource("queryMessages")
    void shouldDispatchingQueryMessagesWhenNotTerminated(QueryMessage queryMessage) {
        // Given
        var channel = newChannel();
        var connection = newConnection(channel);
        var lockingExecutor = mock(TerminationAwareStateLockingExecutor.class);
        doAnswer(invocationOnMock -> {
                    @SuppressWarnings("unchecked")
                    var consumer = (Consumer<Throwable>) invocationOnMock.getArguments()[0];
                    consumer.accept(null);
                    return null;
                })
                .when(lockingExecutor)
                .execute(any());
        connection.bindTerminationAwareStateLockingExecutor(lockingExecutor);
        var handler = mock(ResponseHandler.class);

        // When
        if (queryMessage.flush()) {
            connection.writeAndFlush(queryMessage.message(), handler);
        } else {
            connection.write(queryMessage.message(), handler);
            channel.flushOutbound();
        }
        channel.runPendingTasks();

        // Then
        assertEquals(1, channel.outboundMessages().size());
        then(lockingExecutor).should().execute(any());
    }

    @ParameterizedTest
    @MethodSource("queryMessages")
    void shouldDispatchingQueryMessagesWhenExecutorAbsent(QueryMessage queryMessage) {
        // Given
        var channel = newChannel();
        var connection = newConnection(channel);
        var handler = mock(ResponseHandler.class);

        // When
        if (queryMessage.flush()) {
            connection.writeAndFlush(queryMessage.message(), handler);
        } else {
            connection.write(queryMessage.message(), handler);
            channel.flushOutbound();
        }
        channel.runPendingTasks();

        // Then
        assertEquals(1, channel.outboundMessages().size());
    }

    static List<QueryMessage> queryMessages() {
        return List.of(
                new QueryMessage(false, mock(RunWithMetadataMessage.class)),
                new QueryMessage(true, mock(RunWithMetadataMessage.class)),
                new QueryMessage(false, mock(PullMessage.class)),
                new QueryMessage(true, mock(PullMessage.class)),
                new QueryMessage(false, mock(PullAllMessage.class)),
                new QueryMessage(true, mock(PullAllMessage.class)),
                new QueryMessage(false, mock(DiscardMessage.class)),
                new QueryMessage(true, mock(DiscardMessage.class)),
                new QueryMessage(false, mock(DiscardAllMessage.class)),
                new QueryMessage(true, mock(DiscardAllMessage.class)),
                new QueryMessage(false, mock(CommitMessage.class)),
                new QueryMessage(true, mock(CommitMessage.class)),
                new QueryMessage(false, mock(RollbackMessage.class)),
                new QueryMessage(true, mock(RollbackMessage.class)));
    }

    private record QueryMessage(boolean flush, Message message) {}

    private void testWriteInEventLoop(String threadName, Consumer<NetworkConnection> action) throws Exception {
        var channel = spy(new EmbeddedChannel());
        initializeEventLoop(channel, threadName);
        var dispatcher = new ThreadTrackingInboundMessageDispatcher(channel);
        ChannelAttributes.setProtocolVersion(channel, DEFAULT_TEST_PROTOCOL_VERSION);
        ChannelAttributes.setMessageDispatcher(channel, dispatcher);

        var connection = newConnection(channel);
        action.accept(connection);

        shutdownEventLoop();
        assertThat(single(dispatcher.queueThreadNames), startsWith(threadName));
    }

    private void initializeEventLoop(Channel channel, String namePrefix) {
        executor = Executors.newSingleThreadExecutor(daemon(namePrefix));
        eventLoop = new DefaultEventLoop(executor);
        when(channel.eventLoop()).thenReturn(eventLoop);
    }

    private void shutdownEventLoop() throws Exception {
        if (eventLoop != null) {
            eventLoop.shutdownGracefully();
        }
        if (executor != null) {
            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private static EmbeddedChannel newChannel() {
        var channel = new EmbeddedChannel();
        var messageDispatcher = new InboundMessageDispatcher(channel, DEV_NULL_LOGGING);
        ChannelAttributes.setProtocolVersion(channel, DEFAULT_TEST_PROTOCOL_VERSION);
        ChannelAttributes.setMessageDispatcher(channel, messageDispatcher);
        return channel;
    }

    private static NetworkConnection newConnection(Channel channel) {
        return newConnection(channel, mock(ExtendedChannelPool.class));
    }

    private static NetworkConnection newConnection(Channel channel, ExtendedChannelPool pool) {
        return new NetworkConnection(channel, pool, new FakeClock(), DevNullMetricsListener.INSTANCE, DEV_NULL_LOGGING);
    }

    private static void assertConnectionReleasedError(IllegalStateException e) {
        assertThat(e.getMessage(), startsWith("Connection has been released"));
    }

    private static void assertConnectionTerminatedError(IllegalStateException e) {
        assertThat(e.getMessage(), startsWith("Connection has been terminated"));
    }

    private static class ThreadTrackingInboundMessageDispatcher extends InboundMessageDispatcher {

        final Set<String> queueThreadNames = ConcurrentHashMap.newKeySet();

        ThreadTrackingInboundMessageDispatcher(Channel channel) {
            super(channel, DEV_NULL_LOGGING);
        }

        @Override
        public void enqueue(ResponseHandler handler) {
            queueThreadNames.add(Thread.currentThread().getName());
            super.enqueue(handler);
        }
    }
}
