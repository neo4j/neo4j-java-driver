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
package org.neo4j.driver.internal.reactivestreams;

import java.util.Map;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.reactivestreams.ReactiveQueryRunner;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

interface BaseReactiveQueryRunner extends ReactiveQueryRunner {
    @Override
    default Publisher<ReactiveResult> run(String queryStr, Value parameters) {
        try {
            var query = new Query(queryStr, parameters);
            return run(query);
        } catch (Throwable t) {
            return Mono.error(t);
        }
    }

    @Override
    default Publisher<ReactiveResult> run(String query, Map<String, Object> parameters) {
        return run(query, parameters(parameters));
    }

    @Override
    default Publisher<ReactiveResult> run(String query, Record parameters) {
        return run(query, parameters(parameters));
    }

    @Override
    default Publisher<ReactiveResult> run(String queryStr) {
        try {
            var query = new Query(queryStr);
            return run(query);
        } catch (Throwable t) {
            return Mono.error(t);
        }
    }

    static Value parameters(Record record) {
        return record == null ? Values.EmptyMap : parameters(record.asMap());
    }

    static Value parameters(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return Values.EmptyMap;
        }
        return new MapValue(Extract.mapOfValues(map));
    }
}
