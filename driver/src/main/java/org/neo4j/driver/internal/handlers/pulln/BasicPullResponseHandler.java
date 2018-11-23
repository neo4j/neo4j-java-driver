/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal.handlers.pulln;

import org.reactivestreams.Subscription;

import java.util.function.BiConsumer;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

public interface BasicPullResponseHandler extends ResponseHandler, Subscription
{
    BiConsumer<Record,Throwable> NULL_RECORD_CONSUMER = ( record, throwable ) -> {
    };
    BiConsumer<ResultSummary,Throwable> NULL_SUCCESS_CONSUMER = ( resultSummary, throwable ) -> {
    };

    /**
     * Register a record consumer for each record received.
     * This consumer shall not be registered after streaming started.
     * A null record with no error indicates the termination of streaming.
     * @param recordConsumer register a record consumer to be called back for each record received.
     */
    void installRecordConsumer( BiConsumer<Record,Throwable> recordConsumer );

    /**
     * Register a summary consumer to receive callbacks when a summary is received.
     * The registered success consumer will be called back if
     * 1) a success message with a summary is received,
     * 2) a success message with has_more is received,
     * 3) a failure message is received.
     * This consumer shall not be registered after streaming started.
     * Otherwise the summary might have arrived before the consumer is registered.
     * @param summaryConsumer register a summary consumer
     */
    void installSummaryConsumer( BiConsumer<ResultSummary,Throwable> summaryConsumer );

    /**
     * If the streaming is finished normally or with an error or cancelled.
     * @return True if the stream is finished or cancelled.
     */
    boolean isFinishedOrCanceled();

    /**
     * If the server is not sending more records until another {@link Subscription#request(long)}, but the streaming has not been finished.
     * @return Ture if the stream is paused.
     */
    boolean isPaused();

    enum Status
    {
        Done,
        Failed,
        Cancelled,
        Streaming,
        Paused
    }
}
