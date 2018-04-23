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

import java.time.Duration;
import java.time.Period;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.List;
import java.util.Objects;

import org.neo4j.driver.v1.types.IsoDuration;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class InternalIsoDuration implements IsoDuration
{
    private static final List<TemporalUnit> SUPPORTED_UNITS = unmodifiableList( asList( MONTHS, DAYS, SECONDS, NANOS ) );

    private final long months;
    private final long days;
    private final long seconds;
    private final int nanoseconds;

    public InternalIsoDuration( Period period )
    {
        this( period.toTotalMonths(), period.getDays(), 0, 0 );
    }

    public InternalIsoDuration( Duration duration )
    {
        this( 0, 0, duration.getSeconds(), duration.getNano() );
    }

    public InternalIsoDuration( long months, long days, long seconds, int nanoseconds )
    {
        this.months = months;
        this.days = days;
        this.seconds = seconds;
        this.nanoseconds = nanoseconds;
    }

    public InternalIsoDuration( Period period, Duration duration )
    {
        this( period.toTotalMonths(), period.getDays(), duration.getSeconds(), duration.getNano() );
    }

    @Override
    public long months()
    {
        return months;
    }

    @Override
    public long days()
    {
        return days;
    }

    @Override
    public long seconds()
    {
        return seconds;
    }

    @Override
    public int nanoseconds()
    {
        return nanoseconds;
    }

    @Override
    public long get( TemporalUnit unit )
    {
        if ( unit == MONTHS )
        {
            return months;
        }
        else if ( unit == DAYS )
        {
            return days;
        }
        else if ( unit == SECONDS )
        {
            return seconds;
        }
        else if ( unit == NANOS )
        {
            return nanoseconds;
        }
        else
        {
            throw new UnsupportedTemporalTypeException( "Unsupported unit: " + unit );
        }
    }

    @Override
    public List<TemporalUnit> getUnits()
    {
        return SUPPORTED_UNITS;
    }

    @Override
    public Temporal addTo( Temporal temporal )
    {
        if ( months != 0 )
        {
            temporal = temporal.plus( months, MONTHS );
        }
        if ( days != 0 )
        {
            temporal = temporal.plus( days, DAYS );
        }
        if ( seconds != 0 )
        {
            temporal = temporal.plus( seconds, SECONDS );
        }
        if ( nanoseconds != 0 )
        {
            temporal = temporal.plus( nanoseconds, NANOS );
        }
        return temporal;
    }

    @Override
    public Temporal subtractFrom( Temporal temporal )
    {
        if ( months != 0 )
        {
            temporal = temporal.minus( months, MONTHS );
        }
        if ( days != 0 )
        {
            temporal = temporal.minus( days, DAYS );
        }
        if ( seconds != 0 )
        {
            temporal = temporal.minus( seconds, SECONDS );
        }
        if ( nanoseconds != 0 )
        {
            temporal = temporal.minus( nanoseconds, NANOS );
        }
        return temporal;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        InternalIsoDuration that = (InternalIsoDuration) o;
        return months == that.months &&
               days == that.days &&
               seconds == that.seconds &&
               nanoseconds == that.nanoseconds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( months, days, seconds, nanoseconds );
    }

    @Override
    public String toString()
    {
        return String.format( "P%sM%sDT%s.%sS", months, days, seconds, String.format( "%09d", nanoseconds ) );
    }
}
