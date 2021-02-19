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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.exceptions.value.Uncoercible;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DateTimeValueTest
{
    @Test
    void shouldHaveCorrectType()
    {
        ZonedDateTime dateTime = ZonedDateTime.of( 1991, 2, 24, 12, 0, 0, 999_000, ZoneOffset.ofHours( -5 ) );
        DateTimeValue dateTimeValue = new DateTimeValue( dateTime );
        assertEquals( InternalTypeSystem.TYPE_SYSTEM.DATE_TIME(), dateTimeValue.type() );
    }

    @Test
    void shouldSupportAsObject()
    {
        ZonedDateTime dateTime = ZonedDateTime.of( 2015, 8, 2, 23, 59, 59, 999_999, ZoneId.of( "Europe/Stockholm" ) );
        DateTimeValue dateTimeValue = new DateTimeValue( dateTime );
        assertEquals( dateTime, dateTimeValue.asObject() );
    }

    @Test
    void shouldSupportAsZonedDateTime()
    {
        ZonedDateTime dateTime = ZonedDateTime.of( 1822, 9, 24, 9, 23, 57, 123, ZoneOffset.ofHoursMinutes( 12, 15 ) );
        DateTimeValue dateTimeValue = new DateTimeValue( dateTime );
        assertEquals( dateTime, dateTimeValue.asZonedDateTime() );
    }

    @Test
    void shouldSupportAsOffsetDateTime()
    {
        ZonedDateTime dateTimeWithOffset = ZonedDateTime.of( 2019, 1, 2, 3, 14, 22, 100, ZoneOffset.ofHours( -5 ) );
        DateTimeValue dateTimeValue1 = new DateTimeValue( dateTimeWithOffset );
        assertEquals( dateTimeWithOffset.toOffsetDateTime(), dateTimeValue1.asOffsetDateTime() );

        ZonedDateTime dateTimeWithZoneId = ZonedDateTime.of( 2000, 11, 8, 5, 57, 59, 1, ZoneId.of( "Europe/Stockholm" ) );
        DateTimeValue dateTimeValue2 = new DateTimeValue( dateTimeWithZoneId );
        assertEquals( dateTimeWithZoneId.toOffsetDateTime(), dateTimeValue2.asOffsetDateTime() );
    }

    @Test
    void shouldNotSupportAsLong()
    {
        ZonedDateTime dateTime = ZonedDateTime.now();
        DateTimeValue dateTimeValue = new DateTimeValue( dateTime );

        assertThrows( Uncoercible.class, dateTimeValue::asLong );
    }
}
