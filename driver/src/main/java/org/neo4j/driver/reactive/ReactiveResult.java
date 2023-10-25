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
package org.neo4j.driver.reactive;

import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.summary.ResultSummary;

/**
 * A reactive result provides a reactive way to execute query on the server and receives records back. This reactive result consists of a result key publisher,
 * a record publisher and a result summary publisher. The reactive result is created via {@link ReactiveSession#run(Query)} and {@link
 * ReactiveTransaction#run(Query)} for example. On the creation of the result, the query submitted to create this result will not be executed until one of the
 * publishers in this class is subscribed. The records or the summary stream has to be consumed and finished (completed or errored) to ensure the resources used
 * by this result to be freed correctly.
 *
 * @see Publisher
 * @see Subscriber
 * @see Subscription
 * @since 5.0
 */
public interface ReactiveResult {
    /**
     * Returns a list of keys.
     *
     * @return a list of keys.
     */
    List<String> keys();

    /**
     * Returns a cold unicast publisher of records.
     * <p>
     * When the record publisher is {@linkplain Publisher#subscribe(Subscriber) subscribed}, the query is executed and the result is streamed back as a record
     * stream followed by a result summary. This record publisher publishes all records in the result and signals the completion. However before completion or
     * error reporting if any, a cleanup of result resources such as network connection will be carried out automatically.
     * <p>
     * Therefore the {@link Subscriber} of this record publisher shall wait for the termination signal (complete or error) to ensure that the resources used by
     * this result are released correctly. Then the session is ready to be used to run more queries.
     * <p>
     * Cancelling of the record streaming will immediately terminate the propagation of new records. But it will not cancel query execution on the server. When
     * the execution is finished, the {@link Subscriber} will be notified with a termination signal (complete or error).
     * <p>
     * The record publishing event by default runs in an Network IO thread, as a result no blocking operation is allowed in this thread. Otherwise network IO
     * might be blocked by application logic.
     * <p>
     * This publisher can only be subscribed by one {@link Subscriber} once.
     * <p>
     * If this publisher is subscribed after {@link #keys()}, then the publish of records is carried out after the arrival of keys. If this publisher is
     * subscribed after {@link #consume()}, then a {@link ResultConsumedException} will be thrown.
     *
     * @return a cold unicast publisher of records.
     */
    Publisher<Record> records();

    /**
     * Returns a cold publisher of result summary which arrives after all records.
     * <p>
     * {@linkplain Publisher#subscribe(Subscriber) Subscribing} the summary publisher results in the execution of the query followed by the result summary being
     * returned. The summary publisher cancels record publishing if not yet subscribed and directly streams back the summary on query execution completion. As a
     * result, the invocation of {@link #records()} after this method, would receive an {@link ResultConsumedException}.
     * <p>
     * If subscribed after {@link #keys()}, then the result summary will be published after the query execution without streaming any record to client. If
     * subscribed after {@link #records()}, then the result summary will be published after the query execution and the streaming of records.
     * <p>
     * Usually, this method shall be chained after {@link #records()} to ensure that all records are processed before summary.
     * <p>
     * This method can be subscribed multiple times. When the {@linkplain ResultSummary summary} arrives, it will be buffered locally for all subsequent calls.
     *
     * @return a cold publisher of result summary which only arrives after all records.
     */
    Publisher<ResultSummary> consume();

    /**
     * Determine if result is open.
     * <p>
     * Result is considered to be open if it has not been consumed ({@link #consume()}) and its creator object (e.g. session or transaction) has not been closed
     * (including committed or rolled back).
     * <p>
     * Attempts to access data on closed result will produce {@link ResultConsumedException}.
     *
     * @return a publisher emitting {@code true} if result is open and {@code false} otherwise.
     */
    Publisher<Boolean> isOpen();
}
