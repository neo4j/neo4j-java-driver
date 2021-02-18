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
package org.neo4j.driver.reactive;

import org.neo4j.driver.Query;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;

import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.summary.ResultSummary;

/**
 * A reactive result provides a reactive way to execute query on the server and receives records back.
 * This reactive result consists of a result key publisher, a record publisher and a result summary publisher.
 * The reactive result is created via {@link RxSession#run(Query)} and {@link RxTransaction#run(Query)} for example.
 * On the creation of the result, the query submitted to create this result will not be executed until one of the publishers in this class is subscribed.
 * The records or the summary stream has to be consumed and finished (completed or errored) to ensure the resources used by this result to be freed correctly.
 *
 * @see Publisher
 * @see Subscriber
 * @see Subscription
 * @since 4.0
 */
public interface RxResult
{
    /**
     * Returns a cold publisher of keys.
     * This publisher always publishes one item - a list of keys. The list could be empty which indicates no keys in the result.
     * <p>
     * When this publisher is {@linkplain Publisher#subscribe(Subscriber) subscribed}, the query is sent to the server and executed.
     * This method does not start the record streaming nor publish query execution result.
     * To retrieve the execution result, either {@link #records()} or {@link #consume()} can be used.
     * {@link #records()} starts record streaming and reports query execution result.
     * {@link #consume()} skips record streaming and directly reports query execution result.
     * <p>
     * Consuming of execution result ensures the resources (such as network connections) used by this result is freed correctly.
     * Consuming the keys without consuming the execution result will result in resource leak.
     * To avoid the resource leak, {@link RxSession#close()} (and/or {@link RxTransaction#commit()} and {@link RxTransaction#rollback()}) shall be invoked
     * and subscribed to enforce the result resources created in the {@link RxSession} (and/or {@link RxTransaction}) to be freed correctly.
     * <p>
     * This publisher can be subscribed many times. The keys published stays the same as the keys are buffered.
     * If this publisher is subscribed after the publisher of {@link #records()} or {@link #consume()},
     * then the buffered keys will be returned.
     * @return a cold publisher of keys.
     */
    Publisher<List<String>> keys();

    /**
     * Returns a cold unicast publisher of records.
     * <p>
     * When the record publisher is {@linkplain Publisher#subscribe(Subscriber) subscribed},
     * the query is executed and the result is streamed back as a record stream followed by a result summary.
     * This record publisher publishes all records in the result and signals the completion.
     * However before completion or error reporting if any, a cleanup of result resources such as network connection will be carried out automatically.
     * <p>
     * Therefore the {@link Subscriber} of this record publisher shall wait for the termination signal (complete or error)
     * to ensure that the resources used by this result are released correctly.
     * Then the session is ready to be used to run more queries.
     * <p>
     * Cancelling of the record streaming will immediately terminate the propagation of new records.
     * But it will not cancel query execution on the server.
     * When the execution is finished, the {@link Subscriber} will be notified with a termination signal (complete or error).
     * <p>
     * The record publishing event by default runs in an Network IO thread, as a result no blocking operation is allowed in this thread.
     * Otherwise network IO might be blocked by application logic.
     * <p>
     * This publisher can only be subscribed by one {@link Subscriber} once.
     * <p>
     * If this publisher is subscribed after {@link #keys()}, then the publish of records is carried out after the arrival of keys.
     * If this publisher is subscribed after {@link #consume()}, then a {@link ResultConsumedException} will be thrown.
     * @return a cold unicast publisher of records.
     */
    Publisher<Record> records();

    /**
     * Returns a cold publisher of result summary which arrives after all records.
     * <p>
     * {@linkplain Publisher#subscribe(Subscriber) Subscribing} the summary publisher results in the execution of the query followed by the result summary being returned.
     * The summary publisher cancels record publishing if not yet subscribed and directly streams back the summary on query execution completion.
     * As a result, the invocation of {@link #records()} after this method, would receive an {@link ResultConsumedException}.
     * <p>
     * If subscribed after {@link #keys()}, then the result summary will be published after the query execution without streaming any record to client.
     * If subscribed after {@link #records()}, then the result summary will be published after the query execution and the streaming of records.
     * <p>
     * Usually, this method shall be chained after {@link #records()} to ensure that all records are processed before summary.
     * <p>
     * This method can be subscribed multiple times. When the {@linkplain ResultSummary summary} arrives, it will be buffered locally for all subsequent calls.
     * @return a cold publisher of result summary which only arrives after all records.
     */
    Publisher<ResultSummary> consume();
}
