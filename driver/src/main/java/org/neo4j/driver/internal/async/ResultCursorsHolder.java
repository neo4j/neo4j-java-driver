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
package org.neo4j.driver.internal.async;

import static org.neo4j.driver.internal.util.Futures.completedWithNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.internal.FailableCursor;

public class ResultCursorsHolder {
    private final List<CompletionStage<? extends FailableCursor>> cursorStages = new ArrayList<>();

    void add(CompletionStage<? extends FailableCursor> cursorStage) {
        Objects.requireNonNull(cursorStage);
        synchronized (this) {
            cursorStages.add(cursorStage);
        }
        cursorStage.thenCompose(FailableCursor::consumed).whenComplete((ignored, throwable) -> {
            synchronized (this) {
                cursorStages.remove(cursorStage);
            }
        });
    }

    CompletionStage<Throwable> retrieveNotConsumedError() {
        List<CompletionStage<? extends FailableCursor>> cursorStages;
        synchronized (this) {
            cursorStages = List.copyOf(this.cursorStages);
        }
        var failures = retrieveAllFailures(cursorStages);
        return CompletableFuture.allOf(failures).thenApply(ignore -> findFirstFailure(failures));
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Throwable>[] retrieveAllFailures(
            List<CompletionStage<? extends FailableCursor>> cursorStages) {
        return cursorStages.stream()
                .map(ResultCursorsHolder::retrieveFailure)
                .map(CompletionStage::toCompletableFuture)
                .toArray(CompletableFuture[]::new);
    }

    private static Throwable findFirstFailure(CompletableFuture<Throwable>[] completedFailureFutures) {
        // all given futures should be completed, it is thus safe to get their values

        return Arrays.stream(completedFailureFutures)
                .map(failureFuture -> failureFuture.getNow(null))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    private static CompletionStage<Throwable> retrieveFailure(CompletionStage<? extends FailableCursor> cursorStage) {
        return cursorStage
                .exceptionally(cursor -> null)
                .thenCompose(cursor -> cursor == null ? completedWithNull() : cursor.discardAllFailureAsync());
    }
}
