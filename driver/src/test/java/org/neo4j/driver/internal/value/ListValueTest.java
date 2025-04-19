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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.Values.value;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.types.InternalTypeSystem;

class ListValueTest {
    @Test
    void shouldHaveSensibleToString() {
        var listValue = listValue(value(1), value(2), value(3));
        assertThat(listValue.toString(), equalTo("[1, 2, 3]"));
    }

    @Test
    void shouldHaveCorrectType() {

        var listValue = listValue();

        assertThat(listValue.type(), equalTo(InternalTypeSystem.TYPE_SYSTEM.LIST()));
    }

    @Test
    void shouldMapToType() {
        var list = List.of(0L);
        var values = Values.value(list);
        assertEquals(list, values.as(List.class));
        assertEquals(list, values.as(Collection.class));
        assertEquals(list, values.as(Iterable.class));
        assertEquals(list, values.as(Object.class));
    }

    @Test
    void shouldMapCharValuesToArray() {
        var array = new char[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        var values = Values.value(array);
        assertArrayEquals(array, values.as(char[].class));
    }

    @Test
    void shouldMapBooleanValuesToArray() {
        var array = new boolean[] {false};
        var values = Values.value(array);
        assertArrayEquals(array, values.as(boolean[].class));
    }

    @Test
    void shouldMapStringValuesToArray() {
        var array = new String[] {"value"};
        var values = Values.value(array);
        assertArrayEquals(array, values.as(String[].class));
    }

    @Test
    void shouldMapLongValuesToArray() {
        var array = new long[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        var values = Values.value(array);
        assertArrayEquals(array, values.as(long[].class));
        assertArrayEquals(Arrays.stream(array).mapToInt(l -> (int) l).toArray(), values.as(int[].class));
        assertArrayEquals(Arrays.stream(array).mapToDouble(l -> (double) l).toArray(), values.as(double[].class));
        var expectedFloats = new float[array.length];
        for (var i = 0; i < array.length; i++) expectedFloats[i] = (float) array[i];
        assertArrayEquals(expectedFloats, values.as(float[].class));
    }

    @Test
    void shouldMapIntegerValuesToArray() {
        var array = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        var values = Values.value(array);
        assertArrayEquals(array, values.as(int[].class));
        assertArrayEquals(Arrays.stream(array).mapToLong(l -> (long) l).toArray(), values.as(long[].class));
        assertArrayEquals(Arrays.stream(array).mapToDouble(l -> (double) l).toArray(), values.as(double[].class));
        var expectedFloats = new float[array.length];
        for (var i = 0; i < array.length; i++) expectedFloats[i] = (float) array[i];
        assertArrayEquals(expectedFloats, values.as(float[].class));
    }

    @Test
    void shouldMapShortValuesToArray() {
        var array = new short[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        var values = Values.value(array);
        assertArrayEquals(array, values.as(short[].class));
    }

    @Test
    void shouldMapDoubleValuesToArray() {
        var array = new double[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        var values = Values.value(array);
        assertArrayEquals(array, values.as(double[].class));
        assertArrayEquals(Arrays.stream(array).mapToLong(l -> (long) l).toArray(), values.as(long[].class));
        assertArrayEquals(Arrays.stream(array).mapToInt(l -> (int) l).toArray(), values.as(int[].class));
        var expectedFloats = new float[array.length];
        for (var i = 0; i < array.length; i++) expectedFloats[i] = (float) array[i];
        assertArrayEquals(expectedFloats, values.as(float[].class));
    }

    @Test
    void shouldMapFloatValuesToArray() {
        var array = new float[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        var values = Values.value(array);
        assertArrayEquals(array, values.as(float[].class));
        var expectedDoubles = new double[array.length];
        for (var i = 0; i < array.length; i++) expectedDoubles[i] = array[i];
        assertArrayEquals(expectedDoubles, values.as(double[].class));
        var expectedLongs = new long[array.length];
        for (var i = 0; i < array.length; i++) expectedLongs[i] = (long) array[i];
        assertArrayEquals(expectedLongs, values.as(long[].class));
        var expectedIntegers = new int[array.length];
        for (var i = 0; i < array.length; i++) expectedIntegers[i] = (int) array[i];
        assertArrayEquals(expectedIntegers, values.as(int[].class));
    }

    @Test
    void shouldMapObjectsToArray() {
        var string = "value";
        var localDateTime = LocalDateTime.now();
        var array = new Object[] {string, localDateTime};
        var values = Values.value(array);
        assertArrayEquals(array, values.as(Object[].class));
    }

    @Test
    void shouldMapMatrixValuesToArrays() {
        var array = new long[10][10];
        for (var i = 0; i < array.length; i++) {
            for (var j = 0; j < 10; j++) {
                array[i][j] = i * j;
            }
        }
        var values = Values.value(array);
        assertArrayEquals(array, values.as(long[][].class));
    }

    private ListValue listValue(Value... values) {
        return new ListValue(values);
    }
}
