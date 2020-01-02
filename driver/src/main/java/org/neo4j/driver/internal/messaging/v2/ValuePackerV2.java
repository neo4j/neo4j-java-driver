/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.driver.internal.messaging.v2;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.messaging.v1.ValuePackerV1;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;

import static java.time.ZoneOffset.UTC;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.DATE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.DATE_STRUCT_SIZE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.DATE_TIME_STRUCT_SIZE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.DATE_TIME_WITH_ZONE_ID;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.DATE_TIME_WITH_ZONE_OFFSET;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.DURATION;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.DURATION_TIME_STRUCT_SIZE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.LOCAL_DATE_TIME;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.LOCAL_DATE_TIME_STRUCT_SIZE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.LOCAL_TIME;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.LOCAL_TIME_STRUCT_SIZE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.POINT_2D_STRUCT_SIZE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.POINT_2D_STRUCT_TYPE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.POINT_3D_STRUCT_SIZE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.POINT_3D_STRUCT_TYPE;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.TIME;
import static org.neo4j.driver.internal.messaging.v2.MessageFormatV2.TIME_STRUCT_SIZE;

public class ValuePackerV2 extends ValuePackerV1
{
    public ValuePackerV2( PackOutput output )
    {
        super( output );
    }

    @Override
    protected void packInternalValue( InternalValue value ) throws IOException
    {
        TypeConstructor typeConstructor = value.typeConstructor();
        switch ( typeConstructor )
        {
        case DATE:
            packDate( value.asLocalDate() );
            break;
        case TIME:
            packTime( value.asOffsetTime() );
            break;
        case LOCAL_TIME:
            packLocalTime( value.asLocalTime() );
            break;
        case LOCAL_DATE_TIME:
            packLocalDateTime( value.asLocalDateTime() );
            break;
        case DATE_TIME:
            packZonedDateTime( value.asZonedDateTime() );
            break;
        case DURATION:
            packDuration( value.asIsoDuration() );
            break;
        case POINT:
            packPoint( value.asPoint() );
            break;
        default:
            super.packInternalValue( value );
        }
    }

    private void packDate( LocalDate localDate ) throws IOException
    {
        packer.packStructHeader( DATE_STRUCT_SIZE, DATE );
        packer.pack( localDate.toEpochDay() );
    }

    private void packTime( OffsetTime offsetTime ) throws IOException
    {
        long nanoOfDayLocal = offsetTime.toLocalTime().toNanoOfDay();
        int offsetSeconds = offsetTime.getOffset().getTotalSeconds();

        packer.packStructHeader( TIME_STRUCT_SIZE, TIME );
        packer.pack( nanoOfDayLocal );
        packer.pack( offsetSeconds );
    }

    private void packLocalTime( LocalTime localTime ) throws IOException
    {
        packer.packStructHeader( LOCAL_TIME_STRUCT_SIZE, LOCAL_TIME );
        packer.pack( localTime.toNanoOfDay() );
    }

    private void packLocalDateTime( LocalDateTime localDateTime ) throws IOException
    {
        long epochSecondUtc = localDateTime.toEpochSecond( UTC );
        int nano = localDateTime.getNano();

        packer.packStructHeader( LOCAL_DATE_TIME_STRUCT_SIZE, LOCAL_DATE_TIME );
        packer.pack( epochSecondUtc );
        packer.pack( nano );
    }

    private void packZonedDateTime( ZonedDateTime zonedDateTime ) throws IOException
    {
        long epochSecondLocal = zonedDateTime.toLocalDateTime().toEpochSecond( UTC );
        int nano = zonedDateTime.getNano();

        ZoneId zone = zonedDateTime.getZone();
        if ( zone instanceof ZoneOffset )
        {
            int offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();

            packer.packStructHeader( DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_OFFSET );
            packer.pack( epochSecondLocal );
            packer.pack( nano );
            packer.pack( offsetSeconds );
        }
        else
        {
            String zoneId = zone.getId();

            packer.packStructHeader( DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_ID );
            packer.pack( epochSecondLocal );
            packer.pack( nano );
            packer.pack( zoneId );
        }
    }

    private void packDuration( IsoDuration duration ) throws IOException
    {
        packer.packStructHeader( DURATION_TIME_STRUCT_SIZE, DURATION );
        packer.pack( duration.months() );
        packer.pack( duration.days() );
        packer.pack( duration.seconds() );
        packer.pack( duration.nanoseconds() );
    }

    private void packPoint( Point point ) throws IOException
    {
        if ( point instanceof InternalPoint2D )
        {
            packPoint2D( point );
        }
        else if ( point instanceof InternalPoint3D )
        {
            packPoint3D( point );
        }
        else
        {
            throw new IOException( String.format( "Unknown type: type: %s, value: %s", point.getClass(), point.toString() ) );
        }
    }

    private void packPoint2D( Point point ) throws IOException
    {
        packer.packStructHeader( POINT_2D_STRUCT_SIZE, POINT_2D_STRUCT_TYPE );
        packer.pack( point.srid() );
        packer.pack( point.x() );
        packer.pack( point.y() );
    }

    private void packPoint3D( Point point ) throws IOException
    {
        packer.packStructHeader( POINT_3D_STRUCT_SIZE, POINT_3D_STRUCT_TYPE );
        packer.pack( point.srid() );
        packer.pack( point.x() );
        packer.pack( point.y() );
        packer.pack( point.z() );
    }
}
