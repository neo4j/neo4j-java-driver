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
package org.neo4j.driver.internal.util;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.util.Iterables.newHashMapWithSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.InternalPair;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.util.Pair;

/**
 * Utility class for extracting data.
 */
public final class Extract {
    private Extract() {
        throw new UnsupportedOperationException();
    }

    public static List<Value> list(Value[] values) {
        return switch (values.length) {
            case 0 -> emptyList();
            case 1 -> singletonList(values[0]);
            default -> List.of(values);
        };
    }

    public static <T> List<T> list(Value[] data, Function<Value, T> mapFunction) {
        var size = data.length;
        switch (size) {
            case 0 -> {
                return emptyList();
            }
            case 1 -> {
                return singletonList(mapFunction.apply(data[0]));
            }
            default -> {
                return Arrays.stream(data).map(mapFunction).toList();
            }
        }
    }

    public static <T> Map<String, T> map(Map<String, Value> data, Function<Value, T> mapFunction) {
        if (data.isEmpty()) {
            return emptyMap();
        } else {
            var size = data.size();
            if (size == 1) {
                var head = data.entrySet().iterator().next();
                return singletonMap(head.getKey(), mapFunction.apply(head.getValue()));
            } else {
                Map<String, T> map = Iterables.newLinkedHashMapWithSize(size);
                for (var entry : data.entrySet()) {
                    map.put(entry.getKey(), mapFunction.apply(entry.getValue()));
                }
                return unmodifiableMap(map);
            }
        }
    }

    public static <T> Map<String, T> map(Record record, Function<Value, T> mapFunction) {
        var size = record.size();
        switch (size) {
            case 0 -> {
                return emptyMap();
            }
            case 1 -> {
                return singletonMap(record.keys().get(0), mapFunction.apply(record.get(0)));
            }
            default -> {
                Map<String, T> map = Iterables.newLinkedHashMapWithSize(size);
                var keys = record.keys();
                for (var i = 0; i < size; i++) {
                    map.put(keys.get(i), mapFunction.apply(record.get(i)));
                }
                return unmodifiableMap(map);
            }
        }
    }

    public static <V> Iterable<Pair<String, V>> properties(
            final MapAccessor map, final Function<Value, V> mapFunction) {
        var size = map.size();
        switch (size) {
            case 0 -> {
                return emptyList();
            }
            case 1 -> {
                var key = map.keys().iterator().next();
                var value = map.get(key);
                return singletonList(InternalPair.of(key, mapFunction.apply(value)));
            }
            default -> {
                List<Pair<String, V>> list = new ArrayList<>(size);
                for (var key : map.keys()) {
                    var value = map.get(key);
                    list.add(InternalPair.of(key, mapFunction.apply(value)));
                }
                return unmodifiableList(list);
            }
        }
    }

    public static <V> List<Pair<String, V>> fields(final Record map, final Function<Value, V> mapFunction) {
        var size = map.keys().size();
        switch (size) {
            case 0 -> {
                return emptyList();
            }
            case 1 -> {
                var key = map.keys().iterator().next();
                var value = map.get(key);
                return singletonList(InternalPair.of(key, mapFunction.apply(value)));
            }
            default -> {
                List<Pair<String, V>> list = new ArrayList<>(size);
                var keys = map.keys();
                for (var i = 0; i < size; i++) {
                    var key = keys.get(i);
                    var value = map.get(i);
                    list.add(InternalPair.of(key, mapFunction.apply(value)));
                }
                return unmodifiableList(list);
            }
        }
    }

    public static Map<String, Value> mapOfValues(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return emptyMap();
        }

        Map<String, Value> result = newHashMapWithSize(map.size());
        for (var entry : map.entrySet()) {
            var value = entry.getValue();
            assertParameter(value);
            result.put(entry.getKey(), value(value));
        }
        return result;
    }

    public static void assertParameter(Object value) {
        if (value instanceof Node || value instanceof NodeValue) {
            var message = "Nodes can't be used as parameters.";
            throw new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null);
        }
        if (value instanceof Relationship || value instanceof RelationshipValue) {
            var message = "Relationships can't be used as parameters.";
            throw new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null);
        }
        if (value instanceof Path || value instanceof PathValue) {
            var message = "Paths can't be used as parameters.";
            throw new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null);
        }
    }
}
