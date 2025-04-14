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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.types.InternalTypeSystem;

class TimeValueTest {
    @Test
    void shouldHaveCorrectType() {
        var time = OffsetTime.now().withOffsetSameInstant(ZoneOffset.ofHoursMinutes(5, 30));
        var timeValue = new TimeValue(time);
        assertEquals(InternalTypeSystem.TYPE_SYSTEM.TIME(), timeValue.type());
    }

    @Test
    void shouldSupportAsObject() {
        var time = OffsetTime.of(19, 0, 10, 1, ZoneOffset.ofHours(-3));
        var timeValue = new TimeValue(time);
        assertEquals(time, timeValue.asObject());
    }

    @Test
    void shouldSupportAsOffsetTime() {
        var time = OffsetTime.of(23, 59, 59, 999_999_999, ZoneOffset.ofHoursMinutes(2, 15));
        var timeValue = new TimeValue(time);
        assertEquals(time, timeValue.asOffsetTime());
    }

    @Test
    void shouldNotSupportAsLong() {
        var time = OffsetTime.now().withOffsetSameInstant(ZoneOffset.ofHours(-5));
        var timeValue = new TimeValue(time);

        assertThrows(Uncoercible.class, timeValue::asLong);
    }

    @Test
    void shouldMapToType() {
        var date = OffsetTime.now();
        var values = Values.value(date);
        assertEquals(date, values.as(OffsetTime.class));
        assertEquals(date, values.as(Temporal.class));
        assertEquals(date, values.as(TemporalAdjuster.class));
        assertEquals(date, values.as(Comparable.class));
        assertEquals(date, values.as(Serializable.class));
        assertEquals(date, values.as(Object.class));
    }
}
