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
package org.neo4j.driver.internal.bolt.routedimpl.util;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.ErrorUtil.addSuppressed;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

public class FutureUtil {
    public static CompletionException asCompletionException(Throwable error) {
        if (error instanceof CompletionException) {
            return ((CompletionException) error);
        }
        return new CompletionException(error);
    }

    public static Throwable completionExceptionCause(Throwable error) {
        if (error instanceof CompletionException) {
            return error.getCause();
        }
        return error;
    }

    @SuppressWarnings("ThrowableNotThrown")
    public static <T> CompletableFuture<T> onErrorContinue(
            CompletableFuture<T> future,
            Throwable errorRecorder,
            Function<Throwable, ? extends CompletionStage<T>> onErrorAction) {
        Objects.requireNonNull(future);
        return future.handle((value, error) -> {
                    if (error != null) {
                        // record error
                        combineErrors(errorRecorder, error);
                        return new CompletionResult<T>(null, error);
                    }
                    return new CompletionResult<>(value, null);
                })
                .thenCompose(result -> {
                    if (result.value != null) {
                        return completedFuture(result.value);
                    } else {
                        return onErrorAction.apply(result.error);
                    }
                });
    }

    @SuppressWarnings({"DuplicatedCode", "UnusedReturnValue"})
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

    public static <T> T joinNowOrElseThrow(
            CompletableFuture<T> future, Supplier<? extends RuntimeException> exceptionSupplier) {
        if (future.isDone()) {
            return future.join();
        } else {
            throw exceptionSupplier.get();
        }
    }

    private record CompletionResult<T>(T value, Throwable error) {}
}
