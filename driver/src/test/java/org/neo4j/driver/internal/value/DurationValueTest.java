/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.types.IsoDuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DurationValueTest
{
    @Test
    void shouldHaveCorrectType()
    {
        IsoDuration duration = newDuration( 1, 2, 3, 4 );
        DurationValue durationValue = new DurationValue( duration );
        assertEquals( InternalTypeSystem.TYPE_SYSTEM.DURATION(), durationValue.type() );
    }

    @Test
    void shouldSupportAsObject()
    {
        IsoDuration duration = newDuration( 11, 22, 33, 44 );
        DurationValue durationValue = new DurationValue( duration );
        assertEquals( duration, durationValue.asObject() );
    }

    @Test
    void shouldSupportAsOffsetTime()
    {
        IsoDuration duration = newDuration( 111, 222, 333, 444 );
        DurationValue durationValue = new DurationValue( duration );
        assertEquals( duration, durationValue.asIsoDuration() );
    }

    @Test
    void shouldNotSupportAsLong()
    {
        IsoDuration duration = newDuration( 1111, 2222, 3333, 4444 );
        DurationValue durationValue = new DurationValue( duration );

        assertThrows( Uncoercible.class, durationValue::asLong );
    }

    private static IsoDuration newDuration( long months, long days, long seconds, int nanoseconds )
    {
        return new InternalIsoDuration( months, days, seconds, nanoseconds );
    }
}
