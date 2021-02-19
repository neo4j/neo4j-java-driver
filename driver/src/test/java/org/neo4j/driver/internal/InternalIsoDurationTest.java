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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.temporal.Temporal;
import java.time.temporal.UnsupportedTemporalTypeException;

import org.neo4j.driver.types.IsoDuration;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InternalIsoDurationTest
{
    @Test
    void shouldExposeMonths()
    {
        IsoDuration duration = newDuration( 42, 1, 2, 3 );
        assertEquals( 42, duration.months() );
        assertEquals( 42, duration.get( MONTHS ) );
    }

    @Test
    void shouldExposeDays()
    {
        IsoDuration duration = newDuration( 1, 42, 2, 3 );
        assertEquals( 42, duration.days() );
        assertEquals( 42, duration.get( DAYS ) );
    }

    @Test
    void shouldExposeSeconds()
    {
        IsoDuration duration = newDuration( 1, 2, 42, 3 );
        assertEquals( 42, duration.seconds() );
        assertEquals( 42, duration.get( SECONDS ) );
    }

    @Test
    void shouldExposeNanoseconds()
    {
        IsoDuration duration = newDuration( 1, 2, 3, 42 );
        assertEquals( 42, duration.nanoseconds() );
        assertEquals( 42, duration.get( NANOS ) );
    }

    @Test
    void shouldFailToGetUnsupportedTemporalUnit()
    {
        IsoDuration duration = newDuration( 1, 2, 3, 4 );

        assertThrows( UnsupportedTemporalTypeException.class, () -> duration.get( YEARS ) );
    }

    @Test
    void shouldExposeSupportedTemporalUnits()
    {
        IsoDuration duration = newDuration( 1, 2, 3, 4 );
        assertEquals( asList( MONTHS, DAYS, SECONDS, NANOS ), duration.getUnits() );
    }

    @Test
    void shouldAddTo()
    {
        IsoDuration duration = newDuration( 1, 2, 3, 4 );
        LocalDateTime dateTime = LocalDateTime.of( 1990, 1, 1, 0, 0, 0, 0 );

        Temporal result = duration.addTo( dateTime );

        assertEquals( LocalDateTime.of( 1990, 2, 3, 0, 0, 3, 4 ), result );
    }

    @Test
    void shouldSubtractFrom()
    {
        IsoDuration duration = newDuration( 4, 3, 2, 1 );
        LocalDateTime dateTime = LocalDateTime.of( 1990, 7, 19, 0, 0, 59, 999 );

        Temporal result = duration.subtractFrom( dateTime );

        assertEquals( LocalDateTime.of( 1990, 3, 16, 0, 0, 57, 998 ), result );
    }

    @Test
    void shouldImplementEqualsAndHashCode()
    {
        IsoDuration duration1 = newDuration( 1, 2, 3, 4 );
        IsoDuration duration2 = newDuration( 1, 2, 3, 4 );

        assertEquals( duration1, duration2 );
        assertEquals( duration1.hashCode(), duration2.hashCode() );
    }

    @Test
    void shouldCreateFromPeriod()
    {
        Period period = Period.of( 3, 5, 12 );

        InternalIsoDuration duration = new InternalIsoDuration( period );

        assertEquals( period.toTotalMonths(), duration.months() );
        assertEquals( period.getDays(), duration.days() );
        assertEquals( 0, duration.seconds() );
        assertEquals( 0, duration.nanoseconds() );
    }

    @Test
    void shouldCreateFromDuration()
    {
        Duration duration = Duration.ofSeconds( 391784, 4879173 );

        InternalIsoDuration isoDuration = new InternalIsoDuration( duration );

        assertEquals( 0, isoDuration.months() );
        assertEquals( 0, isoDuration.days() );
        assertEquals( duration.getSeconds(), isoDuration.seconds() );
        assertEquals( duration.getNano(), isoDuration.nanoseconds() );
    }

    @Test
    void toStringShouldPrintInIsoStandardFormat()
    {
        assertThat( newDuration( 0, 0, 0, 0 ).toString(), equalTo( "P0M0DT0S" ) );
        assertThat( newDuration( 2, 45, 59, 11 ).toString(), equalTo( "P2M45DT59.000000011S" ) );
        assertThat( newDuration( 4, -101, 1, 999 ).toString(), equalTo( "P4M-101DT1.000000999S" ) );
        assertThat( newDuration( -1, 12, -19, 1 ).toString(), equalTo( "P-1M12DT-18.999999999S" ) );
        assertThat( newDuration( 0, 0, -1, 1 ).toString(), equalTo( "P0M0DT-0.999999999S" ) );

        assertThat( new InternalIsoDuration( Period.parse( "P356D" ) ).toString(), equalTo( "P0M356DT0S" ) );
        assertThat( new InternalIsoDuration( Duration.parse( "PT45S" ) ).toString(), equalTo( "P0M0DT45S" ) );

        assertThat( new InternalIsoDuration( 0, 14, Duration.parse( "PT16H12M" ) ).toString(), equalTo( "P0M14DT58320S" ) );
        assertThat( new InternalIsoDuration( 5, 1, Duration.parse( "PT12H" ) ).toString(), equalTo( "P5M1DT43200S" ) );
        assertThat( new InternalIsoDuration( 0, 17, Duration.parse( "PT2H0.111222333S" ) ).toString(), equalTo( "P0M17DT7200.111222333S" ) );

        assertThat( newDuration( 42, 42, 42, 0 ).toString(), equalTo( "P42M42DT42S" ) );
        assertThat( newDuration( 42, 42, -42, 0 ).toString(), equalTo( "P42M42DT-42S" ) );

        assertThat( newDuration( 42, 42, 0, 5 ).toString(), equalTo( "P42M42DT0.000000005S" ) );
        assertThat( newDuration( 42, 42, 0, -5 ).toString(), equalTo( "P42M42DT-0.000000005S" ) );

        assertThat( newDuration( 42, 42, 1, 5 ).toString(), equalTo( "P42M42DT1.000000005S" ) );
        assertThat( newDuration( 42, 42, -1, 5 ).toString(), equalTo( "P42M42DT-0.999999995S" ) );
        assertThat( newDuration( 42, 42, 1, -5 ).toString(), equalTo( "P42M42DT0.999999995S" ) );
        assertThat( newDuration( 42, 42, -1, -5 ).toString(), equalTo( "P42M42DT-1.000000005S" ) );

        assertThat( newDuration( 42, 42, 28, 9 ).toString(), equalTo( "P42M42DT28.000000009S" ) );
        assertThat( newDuration( 42, 42, -28, 9 ).toString(), equalTo( "P42M42DT-27.999999991S" ) );
        assertThat( newDuration( 42, 42, 28, -9 ).toString(), equalTo( "P42M42DT27.999999991S" ) );
        assertThat( newDuration( 42, 42, -28, -9 ).toString(), equalTo( "P42M42DT-28.000000009S" ) );
    }

    private static IsoDuration newDuration( long months, long days, long seconds, int nanoseconds )
    {
        return new InternalIsoDuration( months, days, seconds, nanoseconds );
    }
}
