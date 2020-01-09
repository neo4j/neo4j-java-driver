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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.neo4j.driver.internal.messaging.v1.ValueUnpackerV1;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.Value;

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
import static org.neo4j.driver.Values.isoDuration;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;

public class ValueUnpackerV2 extends ValueUnpackerV1
{
    public ValueUnpackerV2( PackInput input )
    {
        super( input );
    }

    @Override
    protected Value unpackStruct( long size, byte type ) throws IOException
    {
        switch ( type )
        {
        case DATE:
            ensureCorrectStructSize( TypeConstructor.DATE, DATE_STRUCT_SIZE, size );
            return unpackDate();
        case TIME:
            ensureCorrectStructSize( TypeConstructor.TIME, TIME_STRUCT_SIZE, size );
            return unpackTime();
        case LOCAL_TIME:
            ensureCorrectStructSize( TypeConstructor.LOCAL_TIME, LOCAL_TIME_STRUCT_SIZE, size );
            return unpackLocalTime();
        case LOCAL_DATE_TIME:
            ensureCorrectStructSize( TypeConstructor.LOCAL_DATE_TIME, LOCAL_DATE_TIME_STRUCT_SIZE, size );
            return unpackLocalDateTime();
        case DATE_TIME_WITH_ZONE_OFFSET:
            ensureCorrectStructSize( TypeConstructor.DATE_TIME, DATE_TIME_STRUCT_SIZE, size );
            return unpackDateTimeWithZoneOffset();
        case DATE_TIME_WITH_ZONE_ID:
            ensureCorrectStructSize( TypeConstructor.DATE_TIME, DATE_TIME_STRUCT_SIZE, size );
            return unpackDateTimeWithZoneId();
        case DURATION:
            ensureCorrectStructSize( TypeConstructor.DURATION, DURATION_TIME_STRUCT_SIZE, size );
            return unpackDuration();
        case POINT_2D_STRUCT_TYPE:
            ensureCorrectStructSize( TypeConstructor.POINT, POINT_2D_STRUCT_SIZE, size );
            return unpackPoint2D();
        case POINT_3D_STRUCT_TYPE:
            ensureCorrectStructSize( TypeConstructor.POINT, POINT_3D_STRUCT_SIZE, size );
            return unpackPoint3D();
        default:
            return super.unpackStruct( size, type );
        }
    }

    private Value unpackDate() throws IOException
    {
        long epochDay = unpacker.unpackLong();
        return value( LocalDate.ofEpochDay( epochDay ) );
    }

    private Value unpackTime() throws IOException
    {
        long nanoOfDayLocal = unpacker.unpackLong();
        int offsetSeconds = Math.toIntExact( unpacker.unpackLong() );

        LocalTime localTime = LocalTime.ofNanoOfDay( nanoOfDayLocal );
        ZoneOffset offset = ZoneOffset.ofTotalSeconds( offsetSeconds );
        return value( OffsetTime.of( localTime, offset ) );
    }

    private Value unpackLocalTime() throws IOException
    {
        long nanoOfDayLocal = unpacker.unpackLong();
        return value( LocalTime.ofNanoOfDay( nanoOfDayLocal ) );
    }

    private Value unpackLocalDateTime() throws IOException
    {
        long epochSecondUtc = unpacker.unpackLong();
        int nano = Math.toIntExact( unpacker.unpackLong() );
        return value( LocalDateTime.ofEpochSecond( epochSecondUtc, nano, UTC ) );
    }

    private Value unpackDateTimeWithZoneOffset() throws IOException
    {
        long epochSecondLocal = unpacker.unpackLong();
        int nano = Math.toIntExact( unpacker.unpackLong() );
        int offsetSeconds = Math.toIntExact( unpacker.unpackLong() );
        return value( newZonedDateTime( epochSecondLocal, nano, ZoneOffset.ofTotalSeconds( offsetSeconds ) ) );
    }

    private Value unpackDateTimeWithZoneId() throws IOException
    {
        long epochSecondLocal = unpacker.unpackLong();
        int nano = Math.toIntExact( unpacker.unpackLong() );
        String zoneIdString = unpacker.unpackString();
        return value( newZonedDateTime( epochSecondLocal, nano, ZoneId.of( zoneIdString ) ) );
    }

    private Value unpackDuration() throws IOException
    {
        long months = unpacker.unpackLong();
        long days = unpacker.unpackLong();
        long seconds = unpacker.unpackLong();
        int nanoseconds = Math.toIntExact( unpacker.unpackLong() );
        return isoDuration( months, days, seconds, nanoseconds );
    }

    private Value unpackPoint2D() throws IOException
    {
        int srid = Math.toIntExact( unpacker.unpackLong() );
        double x = unpacker.unpackDouble();
        double y = unpacker.unpackDouble();
        return point( srid, x, y );
    }

    private Value unpackPoint3D() throws IOException
    {
        int srid = Math.toIntExact( unpacker.unpackLong() );
        double x = unpacker.unpackDouble();
        double y = unpacker.unpackDouble();
        double z = unpacker.unpackDouble();
        return point( srid, x, y, z );
    }

    private static ZonedDateTime newZonedDateTime( long epochSecondLocal, long nano, ZoneId zoneId )
    {
        Instant instant = Instant.ofEpochSecond( epochSecondLocal, nano );
        LocalDateTime localDateTime = LocalDateTime.ofInstant( instant, UTC );
        return ZonedDateTime.of( localDateTime, zoneId );
    }
}
