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
package org.neo4j.driver.internal;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.value;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.internal.value.NullValue;

class InternalRecordTest {
    @Test
    void accessingUnknownKeyShouldBeNull() {
        var record = createRecord();

        assertThat(record.get("k1"), equalTo(value(0)));
        assertThat(record.get("k2"), equalTo(value(1)));
        assertThat(record.get("k3"), equalTo(NullValue.NULL));
    }

    @Test
    void shouldHaveCorrectSize() {
        var record = createRecord();
        assertThat(record.size(), equalTo(2));
    }

    @Test
    void shouldHaveCorrectFieldIndices() {
        var record = createRecord();
        assertThat(record.index("k1"), equalTo(0));
        assertThat(record.index("k2"), equalTo(1));
    }

    @Test
    void shouldThrowWhenAskingForIndexOfUnknownField() {
        var record = createRecord();
        assertThrows(NoSuchElementException.class, () -> record.index("BATMAN"));
    }

    @Test
    void accessingOutOfBoundsShouldBeNull() {
        var record = createRecord();

        assertThat(record.get(0), equalTo(value(0)));
        assertThat(record.get(1), equalTo(value(1)));
        assertThat(record.get(2), equalTo(NullValue.NULL));
        assertThat(record.get(-37), equalTo(NullValue.NULL));
    }

    @Test
    void testContainsKey() {
        var record = createRecord();

        assertTrue(record.containsKey("k1"));
        assertTrue(record.containsKey("k2"));
        assertFalse(record.containsKey("k3"));
    }

    @Test
    void testIndex() {
        var record = createRecord();

        assertThat(record.index("k1"), equalTo(0));
        assertThat(record.index("k2"), equalTo(1));
    }

    @Test
    void testAsMap() {
        // GIVEN
        var record = createRecord();

        // WHEN
        var map = record.asMap();

        // THEN
        assertThat(map.keySet(), containsInAnyOrder("k1", "k2"));
        assertThat(map.get("k1"), equalTo(0L));
        assertThat(map.get("k2"), equalTo(1L));
    }

    @Test
    void testMapExtraction() {
        // GIVEN
        var record = createRecord();
        Function<Value, Integer> addOne = value -> value.asInt() + 1;

        // WHEN
        var map = Extract.map(record, addOne);

        // THEN
        assertThat(map.keySet(), contains("k1", "k2"));
        assertThat(map.get("k1"), equalTo(1));
        assertThat(map.get("k2"), equalTo(2));
    }

    @Test
    void mapExtractionShouldPreserveIterationOrder() {
        // GIVEN
        var keys = Arrays.asList("k2", "k1");
        var record = new InternalRecord(keys, new Value[] {value(0), value(1)});
        Function<Value, Integer> addOne = value -> value.asInt() + 1;

        // WHEN
        var map = Extract.map(record, addOne);

        // THEN
        assertThat(map.keySet(), contains("k2", "k1"));
        var values = map.values().iterator();
        assertThat(values.next(), equalTo(1));
        assertThat(values.next(), equalTo(2));
    }

    @Test
    void testToString() {
        var record = createRecord();

        assertThat(record.toString(), equalTo("Record<{k1: 0, k2: 1}>"));
    }

    @Test
    void shouldHaveMethodToGetKeys() {
        // GIVEN
        var keys = Arrays.asList("k2", "k1");
        var record = new InternalRecord(keys, new Value[] {value(0), value(1)});

        // WHEN
        var appendedKeys = record.keys();

        // THEN
        assertThat(appendedKeys, equalTo(keys));
    }

    @Test
    void emptyKeysShouldGiveEmptyList() {
        // GIVEN
        List<String> keys = Collections.emptyList();
        var record = new InternalRecord(keys, new Value[] {});

        // WHEN
        var appendedKeys = record.keys();

        // THEN
        assertThat(appendedKeys, equalTo(keys));
    }

    @Test
    void shouldHaveMethodToGetValues() {
        // GIVEN
        var keys = Arrays.asList("k2", "k1");
        var values = new Value[] {value(0), value(1)};
        var record = new InternalRecord(keys, values);

        // WHEN
        var appendedValues = record.values();

        // THEN
        assertThat(appendedValues, equalTo(Arrays.asList(values)));
    }

    @Test
    void emptyValuesShouldGiveEmptyList() {
        // GIVEN
        List<String> keys = Collections.emptyList();
        var values = new Value[] {};
        var record = new InternalRecord(keys, values);

        // WHEN
        var appendedValues = record.values();

        // THEN
        assertThat(appendedValues, equalTo(Arrays.asList(values)));
    }

    private InternalRecord createRecord() {
        var keys = Arrays.asList("k1", "k2");
        return new InternalRecord(keys, new Value[] {value(0), value(1)});
    }
}
