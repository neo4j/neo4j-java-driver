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

import java.time.LocalDateTime;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.exceptions.value.Uncoercible;

import static java.time.Month.AUGUST;
import static java.time.Month.FEBRUARY;
import static java.time.Month.JANUARY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalDateTimeValueTest
{
    @Test
    void shouldHaveCorrectType()
    {
        LocalDateTime dateTime = LocalDateTime.of( 1991, AUGUST, 24, 12, 0, 0 );
        LocalDateTimeValue dateTimeValue = new LocalDateTimeValue( dateTime );
        assertEquals( InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME(), dateTimeValue.type() );
    }

    @Test
    void shouldSupportAsObject()
    {
        LocalDateTime dateTime = LocalDateTime.of( 2015, FEBRUARY, 2, 23, 59, 59, 999_999 );
        LocalDateTimeValue dateTimeValue = new LocalDateTimeValue( dateTime );
        assertEquals( dateTime, dateTimeValue.asObject() );
    }

    @Test
    void shouldSupportAsLocalDateTime()
    {
        LocalDateTime dateTime = LocalDateTime.of( 1822, JANUARY, 24, 9, 23, 57, 123 );
        LocalDateTimeValue dateTimeValue = new LocalDateTimeValue( dateTime );
        assertEquals( dateTime, dateTimeValue.asLocalDateTime() );
    }

    @Test
    void shouldNotSupportAsLong()
    {
        LocalDateTime dateTime = LocalDateTime.now();
        LocalDateTimeValue dateTimeValue = new LocalDateTimeValue( dateTime );

        assertThrows( Uncoercible.class, dateTimeValue::asLong );
    }
}
