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
package org.neo4j.driver;

import java.util.function.Function;

/**
 * Static utility methods for retaining records
 *
 * @see Result#list()
 * @since 1.0
 */
public final class Records {
    private Records() {}

    /**
     * Returns a function mapping record to a value from a given index.
     * @param index the index value
     * @return the function
     */
    public static Function<Record, Value> column(int index) {
        return column(index, Values.ofValue());
    }

    /**
     * Returns a function mapping record to a value from a given key.
     * @param key the key value
     * @return the function
     */
    public static Function<Record, Value> column(String key) {
        return column(key, Values.ofValue());
    }

    /**
     * Returns a function mapping record to a value of a target type from a given index.
     * @param index the index value
     * @param mapFunction the function mapping value to a value of a target type
     * @return the function
     * @param <T> the target type
     */
    public static <T> Function<Record, T> column(final int index, final Function<Value, T> mapFunction) {
        return record -> mapFunction.apply(record.get(index));
    }

    /**
     * Returns a function mapping record to a value of a target type from a given key.
     * @param key the key value
     * @param mapFunction the function mapping value to a value of a target type
     * @return the function
     * @param <T> the target type
     */
    public static <T> Function<Record, T> column(final String key, final Function<Value, T> mapFunction) {
        return recordAccessor -> mapFunction.apply(recordAccessor.get(key));
    }
}
