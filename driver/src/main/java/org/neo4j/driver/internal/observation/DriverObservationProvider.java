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
package org.neo4j.driver.internal.observation;

import java.net.URI;
import java.util.List;
import java.util.function.BiConsumer;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.BaseSession;
import org.neo4j.driver.observation.ObservationProvider;
import org.neo4j.driver.types.MapAccessor;

public interface DriverObservationProvider extends ObservationProvider {
    Observation sessionRun(Class<? extends BaseSession> sessionType, String query, MapAccessor parameters);

    Observation beginTransaction(Class<?> transactionType);

    Observation sessionExecute(Class<? extends BaseSession> sessionType, AccessMode mode);

    Observation sessionClose(Class<? extends BaseSession> sessionType);

    Observation transactionRun(Class<?> transactionType, String query, MapAccessor parameters);

    Observation transactionCommit(Class<?> transactionType);

    Observation transactionRollback(Class<?> transactionType);

    Observation transactionClose(Class<?> transactionType);

    Observation resultPeek(Class<?> resultType);

    Observation resultNext(Class<?> resultType);

    Observation resultSingle(Class<?> resultType);

    Observation resultList(Class<?> resultType);

    Observation resultConsume(Class<?> resultType);

    Observation resultRecords(Class<?> resultType);

    Observation connectionPoolCreate(String id, URI uri, int maxSize);

    Observation connectionPoolClose(String id, URI uri);

    Observation pooledConnectionCreate(String id, URI uri);

    Observation pooledConnectionClose(String id, URI uri);

    Observation pooledConnectionAcquire(String id, URI uri);

    Observation pooledConnectionInUse(String id, URI uri);

    BoltHandleObservation boltHandle(List<String> messageTypes);

    BoltExchangeObservation boltExchange(
            String host, int port, BoltProtocolVersion boltVersion, BiConsumer<String, String> setter);

    HttpExchangeObservation httpExchange(URI uri, String method, String uriTemplate, BiConsumer<String, String> setter);

    Observation scopedObservation();
}
