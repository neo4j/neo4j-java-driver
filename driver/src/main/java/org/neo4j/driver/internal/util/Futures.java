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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.ErrorUtil.addSuppressed;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import org.neo4j.bolt.connection.netty.EventLoopThread;

public final class Futures {
    private static final CompletableFuture<?> COMPLETED_WITH_NULL = completedFuture(null);

    private Futures() {}

    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<T> completedWithNull() {
        return (CompletableFuture<T>) COMPLETED_WITH_NULL;
    }

    public static <V> V blockingGet(CompletionStage<V> stage) {
        return blockingGet(stage, Futures::noOpInterruptHandler);
    }

    public static <V> V blockingGet(CompletionStage<V> stage, Runnable interruptHandler) {
        assertNotInEventLoopThread();

        Future<V> future = stage.toCompletableFuture();
        var interrupted = false;
        try {
            while (true) {
                try {
                    return future.get();
                } catch (InterruptedException e) {
                    // this thread was interrupted while waiting
                    // computation denoted by the future might still be running

                    interrupted = true;

                    // run the interrupt handler and ignore if it throws
                    // need to wait for IO thread to actually finish, can't simply re-rethrow
                    safeRun(interruptHandler);
                } catch (ExecutionException e) {
                    ErrorUtil.rethrowAsyncException(e);
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static <T> T getNow(CompletionStage<T> stage) {
        return stage.toCompletableFuture().getNow(null);
    }

    /**
     * Helper method to extract cause of a {@link CompletionException}.
     * <p>
     * When using {@link CompletionStage#whenComplete(BiConsumer)} and {@link CompletionStage#handle(BiFunction)} propagated exceptions might get wrapped in a
     * {@link CompletionException}.
     *
     * @param error the exception to get cause for.
     * @return cause of the given exception if it is a {@link CompletionException}, given exception otherwise.
     */
    public static Throwable completionExceptionCause(Throwable error) {
        if (error instanceof CompletionException) {
            return error.getCause();
        }
        return error;
    }

    /**
     * Helped method to turn given exception into a {@link CompletionException}.
     *
     * @param error the exception to convert.
     * @return given exception wrapped with {@link CompletionException} if it's not one already.
     */
    public static CompletionException asCompletionException(Throwable error) {
        if (error instanceof CompletionException) {
            return ((CompletionException) error);
        }
        return new CompletionException(error);
    }

    /**
     * Combine given errors into a single {@link CompletionException} to be rethrown from inside a
     * {@link CompletionStage} chain.
     *
     * @param error1 the first error or {@code null}.
     * @param error2 the second error or {@code null}.
     * @return {@code null} if both errors are null, {@link CompletionException} otherwise.
     */
    @SuppressWarnings("DuplicatedCode")
    public static CompletionException combineErrors(Throwable error1, Throwable error2) {
        if (error1 != null && error2 != null) {
            var cause1 = completionExceptionCause(error1);
            var cause2 = completionExceptionCause(error2);
            addSuppressed(cause1, cause2);
            return asCompletionException(cause1);
        } else if (error1 != null) {
            return asCompletionException(error1);
        } else if (error2 != null) {
            return asCompletionException(error2);
        } else {
            return null;
        }
    }

    public static <T> BiConsumer<T, Throwable> futureCompletingConsumer(CompletableFuture<T> future) {
        return (value, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                future.complete(value);
            }
        };
    }

    /**
     * Assert that current thread is not an event loop used for async IO operations. This check is needed because
     * blocking API methods are implemented on top of corresponding async API methods. Deadlocks might happen when
     * IO thread executes blocking API call and has to wait for itself to read from the network.
     *
     * @throws IllegalStateException when current thread is an event loop IO thread.
     */
    public static void assertNotInEventLoopThread() throws IllegalStateException {
        if (isEventLoopThread(Thread.currentThread())) {
            throw new IllegalStateException(
                    "Blocking operation can't be executed in IO thread because it might result in a deadlock. "
                            + "Please do not use blocking API when chaining futures returned by async API methods.");
        }
    }

    /**
     * Check if given thread is an event loop IO thread.
     *
     * @param thread the thread to check.
     * @return {@code true} when given thread belongs to the event loop, {@code false} otherwise.
     */
    public static boolean isEventLoopThread(Thread thread) {
        return thread instanceof EventLoopThread;
    }

    private static void safeRun(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable ignore) {
        }
    }

    @SuppressWarnings("EmptyMethod")
    private static void noOpInterruptHandler() {}
}
