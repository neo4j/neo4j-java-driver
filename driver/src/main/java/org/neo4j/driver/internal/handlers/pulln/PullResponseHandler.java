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
package org.neo4j.driver.internal.handlers.pulln;

import org.reactivestreams.Subscription;

import java.util.function.BiConsumer;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.Record;
import org.neo4j.driver.summary.ResultSummary;

public interface PullResponseHandler extends ResponseHandler, Subscription
{
    /**
     * Register a record consumer for each record received.
     * STREAMING shall not be started before this consumer is registered.
     * A null record with no error indicates the end of streaming.
     * @param recordConsumer register a record consumer to be notified for each record received.
     */
    void installRecordConsumer( BiConsumer<Record,Throwable> recordConsumer );

    /**
     * Register a summary consumer to be notified when a summary is received.
     * STREAMING shall not be started before this consumer is registered.
     * A null summary with no error indicates a SUCCESS message with has_more=true has arrived.
     * @param summaryConsumer register a summary consumer
     */
    void installSummaryConsumer( BiConsumer<ResultSummary,Throwable> summaryConsumer );

}
