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
import static org.mockito.Mockito.mock;

import java.time.temporal.TemporalAmount;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.IsoDuration;

class DurationValueTest {
    @Test
    void shouldHaveCorrectType() {
        var duration = newDuration(1, 2, 3, 4);
        var durationValue = new DurationValue(duration);
        assertEquals(InternalTypeSystem.TYPE_SYSTEM.DURATION(), durationValue.type());
    }

    @Test
    void shouldSupportAsObject() {
        var duration = newDuration(11, 22, 33, 44);
        var durationValue = new DurationValue(duration);
        assertEquals(duration, durationValue.asObject());
    }

    @Test
    void shouldSupportAsOffsetTime() {
        var duration = newDuration(111, 222, 333, 444);
        var durationValue = new DurationValue(duration);
        assertEquals(duration, durationValue.asIsoDuration());
    }

    @Test
    void shouldNotSupportAsLong() {
        var duration = newDuration(1111, 2222, 3333, 4444);
        var durationValue = new DurationValue(duration);

        assertThrows(Uncoercible.class, durationValue::asLong);
    }

    @Test
    void shouldMapToType() {
        var date = mock(IsoDuration.class);
        var values = Values.value(date);
        assertEquals(date, values.as(IsoDuration.class));
        assertEquals(date, values.as(TemporalAmount.class));
        assertEquals(date, values.as(Object.class));
    }

    private static IsoDuration newDuration(long months, long days, long seconds, int nanoseconds) {
        return new InternalIsoDuration(months, days, seconds, nanoseconds);
    }
}
