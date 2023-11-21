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
package org.neo4j.driver.internal.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.util.Matchers.blockingOperationInEventLoopError;
import static org.neo4j.driver.testutil.DaemonThreadFactory.daemon;
import static org.neo4j.driver.testutil.TestUtil.sleep;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.FailedFuture;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.SucceededFuture;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.async.connection.EventLoopGroupFactory;

class FuturesTest {
    @Test
    void shouldConvertCanceledNettyFutureToCompletionStage() {
        var promise = new DefaultPromise<String>(ImmediateEventExecutor.INSTANCE);
        promise.cancel(true);

        var future = Futures.asCompletionStage(promise).toCompletableFuture();

        assertTrue(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        assertThrows(CancellationException.class, future::get);
    }

    @Test
    void shouldConvertSucceededNettyFutureToCompletionStage() throws Exception {
        var nettyFuture = new SucceededFuture<>(ImmediateEventExecutor.INSTANCE, "Hello");

        var future = Futures.asCompletionStage(nettyFuture).toCompletableFuture();

        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertEquals("Hello", future.get());
    }

    @Test
    void shouldConvertFailedNettyFutureToCompletionStage() {
        var error = new RuntimeException("Hello");
        var nettyFuture = new FailedFuture<>(ImmediateEventExecutor.INSTANCE, error);

        var future = Futures.asCompletionStage(nettyFuture).toCompletableFuture();

        assertTrue(future.isCompletedExceptionally());
        var e = assertThrows(ExecutionException.class, future::get);
        assertEquals(error, e.getCause());
    }

    @Test
    void shouldConvertRunningNettyFutureToCompletionStageWhenFutureCanceled() {
        var promise = new DefaultPromise<String>(ImmediateEventExecutor.INSTANCE);

        var future = Futures.asCompletionStage(promise).toCompletableFuture();

        assertFalse(future.isDone());
        promise.cancel(true);

        assertTrue(future.isCancelled());
        assertTrue(future.isCompletedExceptionally());
        assertThrows(CancellationException.class, future::get);
    }

    @Test
    void shouldConvertRunningNettyFutureToCompletionStageWhenFutureSucceeded() throws Exception {
        var promise = new DefaultPromise<String>(ImmediateEventExecutor.INSTANCE);

        var future = Futures.asCompletionStage(promise).toCompletableFuture();

        assertFalse(future.isDone());
        promise.setSuccess("Hello");

        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertEquals("Hello", future.get());
    }

    @Test
    void shouldConvertRunningNettyFutureToCompletionStageWhenFutureFailed() {
        var error = new RuntimeException("Hello");
        var promise = new DefaultPromise<String>(ImmediateEventExecutor.INSTANCE);

        var future = Futures.asCompletionStage(promise).toCompletableFuture();

        assertFalse(future.isDone());
        promise.setFailure(error);

        assertTrue(future.isCompletedExceptionally());
        var e = assertThrows(ExecutionException.class, future::get);
        assertEquals(error, e.getCause());
    }

    @Test
    void shouldCreateFailedFutureWithUncheckedException() {
        var error = new RuntimeException("Hello");
        var future = Futures.failedFuture(error).toCompletableFuture();
        assertTrue(future.isCompletedExceptionally());
        var e = assertThrows(ExecutionException.class, future::get);
        assertEquals(error, e.getCause());
    }

    @Test
    void shouldCreateFailedFutureWithCheckedException() {
        var error = new IOException("Hello");
        var future = Futures.failedFuture(error).toCompletableFuture();
        assertTrue(future.isCompletedExceptionally());
        var e = assertThrows(ExecutionException.class, future::get);
        assertEquals(error, e.getCause());
    }

    @Test
    void shouldFailBlockingGetInEventLoopThread() {
        var eventExecutor = EventLoopGroupFactory.newEventLoopGroup(1);
        try {
            var future = new CompletableFuture<String>();
            var result = eventExecutor.submit(() -> Futures.blockingGet(future));

            var e = assertThrows(ExecutionException.class, result::get);
            assertThat(e.getCause(), is(blockingOperationInEventLoopError()));
        } finally {
            eventExecutor.shutdownGracefully();
        }
    }

    @Test
    void shouldThrowInBlockingGetWhenFutureThrowsUncheckedException() {
        var error = new RuntimeException("Hello");

        var future = new CompletableFuture<String>();
        future.completeExceptionally(error);

        var e = assertThrows(Exception.class, () -> Futures.blockingGet(future));
        assertEquals(error, e);
    }

    @Test
    void shouldThrowInBlockingGetWhenFutureThrowsCheckedException() {
        var error = new IOException("Hello");

        var future = new CompletableFuture<String>();
        future.completeExceptionally(error);

        var e = assertThrows(Neo4jException.class, () -> Futures.blockingGet(future));
        assertEquals(error, e.getCause());
    }

    @Test
    void shouldReturnFromBlockingGetWhenFutureCompletes() {
        var future = new CompletableFuture<String>();
        future.complete("Hello");

        assertEquals("Hello", Futures.blockingGet(future));
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    void shouldWaitForFutureInBlockingGetEvenWhenInterrupted() {
        var executor = Executors.newSingleThreadExecutor(daemon("InterruptThread"));
        try {
            var future = new CompletableFuture<String>();

            Thread.currentThread().interrupt();
            executor.submit(() -> {
                sleep(1_000);
                future.complete("Hello");
            });

            assertEquals("Hello", Futures.blockingGet(future));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted(); // clear interruption status
            executor.shutdown();
        }
    }

    @Test
    @SuppressWarnings("ResultOfMethodCallIgnored")
    void shouldHandleInterruptsInBlockingGet() {
        try {
            var future = new CompletableFuture<String>();
            Thread.currentThread().interrupt();

            Runnable interruptHandler = () -> future.complete("Hello");
            assertEquals("Hello", Futures.blockingGet(future, interruptHandler));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted(); // clear interruption status
        }
    }

    @Test
    void shouldGetNowWhenFutureDone() {
        var future = new CompletableFuture<String>();
        future.complete("Hello");

        assertEquals("Hello", Futures.getNow(future));
    }

    @Test
    void shouldGetNowWhenFutureNotDone() {
        var future = new CompletableFuture<String>();

        assertNull(Futures.getNow(future));
    }

    @Test
    void shouldGetCauseFromCompletionException() {
        var error = new RuntimeException("Hello");
        var completionException = new CompletionException(error);

        assertEquals(error, Futures.completionExceptionCause(completionException));
    }

    @Test
    void shouldReturnSameExceptionWhenItIsNotCompletionException() {
        var error = new RuntimeException("Hello");

        assertEquals(error, Futures.completionExceptionCause(error));
    }

    @Test
    void shouldWrapWithCompletionException() {
        var error = new RuntimeException("Hello");
        var completionException = Futures.asCompletionException(error);
        assertEquals(error, completionException.getCause());
    }

    @Test
    void shouldKeepCompletionExceptionAsIs() {
        var error = new CompletionException(new RuntimeException("Hello"));
        assertEquals(error, Futures.asCompletionException(error));
    }

    @Test
    void shouldCombineTwoErrors() {
        var error1 = new RuntimeException("Error1");
        var error2Cause = new RuntimeException("Error2");
        var error2 = new CompletionException(error2Cause);

        var combined = Futures.combineErrors(error1, error2);

        assertEquals(error1, combined.getCause());
        assertArrayEquals(new Throwable[] {error2Cause}, combined.getCause().getSuppressed());
    }

    @Test
    void shouldCombineErrorAndNull() {
        var error1 = new RuntimeException("Error1");

        var combined = Futures.combineErrors(error1, null);

        assertEquals(error1, combined.getCause());
    }

    @Test
    void shouldCombineNullAndError() {
        var error2 = new RuntimeException("Error2");

        var combined = Futures.combineErrors(null, error2);

        assertEquals(error2, combined.getCause());
    }

    @Test
    void shouldCombineNullAndNullErrors() {
        assertNull(Futures.combineErrors(null, null));
    }
}
