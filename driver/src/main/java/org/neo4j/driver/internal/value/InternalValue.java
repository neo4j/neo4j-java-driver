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
package org.neo4j.driver.internal.value;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.Map;
import org.neo4j.bolt.connection.values.IsoDuration;
import org.neo4j.bolt.connection.values.Point;
import org.neo4j.bolt.connection.values.Vector;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.AsValue;
import org.neo4j.driver.internal.types.TypeConstructor;

public interface InternalValue extends Value, AsValue, org.neo4j.bolt.connection.values.Value {
    TypeConstructor typeConstructor();

    default Object as(Type type) {
        if (type instanceof ParameterizedType parameterizedType) {
            return as(parameterizedType);
        } else if (type instanceof Class<?> classType) {
            return as(classType);
        } else {
            throw new Uncoercible(type().name(), type.toString());
        }
    }

    default Object as(ParameterizedType type) {
        throw new Uncoercible(type().name(), type.toString());
    }

    @Override
    default org.neo4j.bolt.connection.values.Value getBoltValue(String key) {
        return (org.neo4j.bolt.connection.values.Value) get(key);
    }

    @Override
    default IsoDuration asBoltIsoDuration() {
        return (IsoDuration) asIsoDuration();
    }

    @Override
    default Point asBoltPoint() {
        return (Point) asPoint();
    }

    @Override
    default Iterable<org.neo4j.bolt.connection.values.Value> boltValues() {
        var values = values();
        return () -> {
            var iterator = values.iterator();
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public org.neo4j.bolt.connection.values.Value next() {
                    var next = iterator.next();
                    return next != null ? (org.neo4j.bolt.connection.values.Value) next : null;
                }
            };
        };
    }

    @Override
    default Map<String, org.neo4j.bolt.connection.values.Value> asBoltMap() {
        return asMap(value -> (org.neo4j.bolt.connection.values.Value) value);
    }

    @Override
    default Vector asBoltVector() {
        throw new UnsupportedOperationException("Vector is not supported");
    }
}
