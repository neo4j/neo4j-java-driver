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
package org.neo4j.driver.internal.cursor;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.FailableCursor;
import org.neo4j.driver.summary.ResultSummary;
import org.reactivestreams.Subscription;

public interface RxResultCursor extends Subscription, FailableCursor {
    List<String> keys();

    void installRecordConsumer(BiConsumer<Record, Throwable> recordConsumer);

    CompletionStage<ResultSummary> summaryAsync();

    boolean isDone();

    Throwable getRunError();

    /**
     * Rolls back this instance by releasing connection with RESET.
     * <p>
     * This must never be called on a published instance.
     * @return reset completion stage
     * @since 5.11
     */
    CompletionStage<Void> rollback();
}
