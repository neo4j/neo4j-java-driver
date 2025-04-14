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
import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.types.InternalTypeSystem;

class DateValueTest {
    @Test
    void shouldHaveCorrectType() {
        var localDate = LocalDate.now();
        var dateValue = new DateValue(localDate);
        assertEquals(InternalTypeSystem.TYPE_SYSTEM.DATE(), dateValue.type());
    }

    @Test
    void shouldSupportAsObject() {
        var localDate = LocalDate.now();
        var dateValue = new DateValue(localDate);
        assertEquals(localDate, dateValue.asObject());
    }

    @Test
    void shouldSupportAsLocalDate() {
        var localDate = LocalDate.now();
        var dateValue = new DateValue(localDate);
        assertEquals(localDate, dateValue.asLocalDate());
    }

    @Test
    void shouldNotSupportAsLong() {
        var localDate = LocalDate.now();
        var dateValue = new DateValue(localDate);

        assertThrows(Uncoercible.class, dateValue::asLong);
    }

    @Test
    void shouldMapToType() {
        var date = LocalDate.now();
        var values = Values.value(date);
        assertEquals(date, values.as(LocalDate.class));
        assertEquals(date, values.as(Temporal.class));
        assertEquals(date, values.as(TemporalAdjuster.class));
        assertEquals(date, values.as(ChronoLocalDate.class));
        assertEquals(date, values.as(Comparable.class));
        assertEquals(date, values.as(Serializable.class));
        assertEquals(date, values.as(Object.class));
    }
}
