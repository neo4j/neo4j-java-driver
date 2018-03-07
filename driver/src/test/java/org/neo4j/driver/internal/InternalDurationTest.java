/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.internal;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.UnsupportedTemporalTypeException;

import org.neo4j.driver.v1.types.Duration;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InternalDurationTest
{
    @Test
    public void shouldExposeMonths()
    {
        Duration duration = newDuration( 42, 1, 2, 3 );
        assertEquals( 42, duration.months() );
        assertEquals( 42, duration.get( MONTHS ) );
    }

    @Test
    public void shouldExposeDays()
    {
        Duration duration = newDuration( 1, 42, 2, 3 );
        assertEquals( 42, duration.days() );
        assertEquals( 42, duration.get( DAYS ) );
    }

    @Test
    public void shouldExposeSeconds()
    {
        Duration duration = newDuration( 1, 2, 42, 3 );
        assertEquals( 42, duration.seconds() );
        assertEquals( 42, duration.get( SECONDS ) );
    }

    @Test
    public void shouldExposeNanoseconds()
    {
        Duration duration = newDuration( 1, 2, 3, 42 );
        assertEquals( 42, duration.nanoseconds() );
        assertEquals( 42, duration.get( NANOS ) );
    }

    @Test
    public void shouldFailToGetUnsupportedTemporalUnit()
    {
        Duration duration = newDuration( 1, 2, 3, 4 );

        try
        {
            duration.get( YEARS );
            fail( "Exception expected" );
        }
        catch ( UnsupportedTemporalTypeException ignore )
        {
        }
    }

    @Test
    public void shouldExposeSupportedTemporalUnits()
    {
        Duration duration = newDuration( 1, 2, 3, 4 );
        assertEquals( asList( MONTHS, DAYS, SECONDS, NANOS ), duration.getUnits() );
    }

    @Test
    public void shouldAddTo()
    {
        Duration duration = newDuration( 1, 2, 3, 4 );
        LocalDateTime dateTime = LocalDateTime.of( 1990, 1, 1, 0, 0, 0, 0 );

        Temporal result = duration.addTo( dateTime );

        assertEquals( LocalDateTime.of( 1990, 2, 3, 0, 0, 3, 4 ), result );
    }

    @Test
    public void shouldSubtractFrom()
    {
        Duration duration = newDuration( 4, 3, 2, 1 );
        LocalDateTime dateTime = LocalDateTime.of( 1990, 7, 19, 0, 0, 59, 999 );

        Temporal result = duration.subtractFrom( dateTime );

        assertEquals( LocalDateTime.of( 1990, 3, 16, 0, 0, 57, 998 ), result );
    }

    @Test
    public void shouldImplementEqualsAndHashCode()
    {
        Duration duration1 = newDuration( 1, 2, 3, 4 );
        Duration duration2 = newDuration( 1, 2, 3, 4 );

        assertEquals( duration1, duration2 );
        assertEquals( duration1.hashCode(), duration2.hashCode() );
    }

    private static Duration newDuration( long months, long days, long seconds, long nanoseconds )
    {
        return new InternalDuration( months, days, seconds, nanoseconds );
    }
}