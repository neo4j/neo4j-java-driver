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

import static java.time.Month.AUGUST;
import static java.time.Month.FEBRUARY;
import static java.time.Month.JANUARY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.chrono.ChronoLocalDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.types.InternalTypeSystem;

class LocalDateTimeValueTest {
    @Test
    void shouldHaveCorrectType() {
        var dateTime = LocalDateTime.of(1991, AUGUST, 24, 12, 0, 0);
        var dateTimeValue = new LocalDateTimeValue(dateTime);
        assertEquals(InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), dateTimeValue.type());
    }

    @Test
    void shouldSupportAsObject() {
        var dateTime = LocalDateTime.of(2015, FEBRUARY, 2, 23, 59, 59, 999_999);
        var dateTimeValue = new LocalDateTimeValue(dateTime);
        assertEquals(dateTime, dateTimeValue.asObject());
    }

    @Test
    void shouldSupportAsLocalDateTime() {
        var dateTime = LocalDateTime.of(1822, JANUARY, 24, 9, 23, 57, 123);
        var dateTimeValue = new LocalDateTimeValue(dateTime);
        assertEquals(dateTime, dateTimeValue.asLocalDateTime());
    }

    @Test
    void shouldNotSupportAsLong() {
        var dateTime = LocalDateTime.now();
        var dateTimeValue = new LocalDateTimeValue(dateTime);

        assertThrows(Uncoercible.class, dateTimeValue::asLong);
    }

    @Test
    void shouldMapToType() {
        var date = LocalDateTime.now();
        var values = Values.value(date);
        assertEquals(date, values.as(LocalDateTime.class));
        assertEquals(date, values.as(Temporal.class));
        assertEquals(date, values.as(TemporalAdjuster.class));
        assertEquals(date, values.as(ChronoLocalDateTime.class));
        assertEquals(date, values.as(Comparable.class));
        assertEquals(date, values.as(Serializable.class));
        assertEquals(date, values.as(Object.class));
    }
}
