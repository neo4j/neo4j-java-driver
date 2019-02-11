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
package org.neo4j.driver.react;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

public interface RxResult
{
    /**
     * TODO: This method currently only start the run, it does not start any streaming.
     * TODO: Thia means if a user forgot to call `records()`, then he will leave this connection with the session.
     * TODO: 1) Bring back `RxSession#close` to avoid the connection left in result.
     * TODO: 2) Change the method to `List<String> keys()`, which returns keys when it is available or IllegalStateException.
     * @return TODO
     */
    Publisher<String> keys();

    /**
     * Returns a cold publisher of records.
     * If the publisher is not subscribed {@link Publisher#subscribe(Subscriber)}, then the query submitted to obtain this result will not run at all.
     * In other words, no network connection creation or traffic for running the query will happen until the publisher is subscribed.
     * The connection used by result will be automatically returned back to the pool once the {@link Subscriber} is terminated.
     * This publisher cannot be subscribed multiple times.
     * @return A cold publisher of records.
     */
    Publisher<Record> records();

    /**
     * Returns a cold publisher of result summary which only arrives after all records.
     * TODO This method currently will not start running or streaming.
     * TODO This is kind of wrong as we are creating a summary publisher that does not have control of streaming of result summary.
     * TODO 1) Calling this method without consuming all records will result in cancellation of record streaming immediately.
     * TODO 2) Change the method to a `ResultSummary summary()`, which returns summary when it is done or throw a IllegalStateException.
     * Usually, this method shall be chained after {@link this#records()} to ensure that all records are processed.
     * This method can be subscribed multiple times. When the {@link ResultSummary} arrives, it will be buffered locally for all subsequent calls.
     * @return a cold publisher of result summary which only arrives after all records.
     */
    Publisher<ResultSummary> summary();
}
