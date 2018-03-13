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
package org.neo4j.driver.internal.messaging;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.IsoDuration;
import org.neo4j.driver.v1.types.Point2D;
import org.neo4j.driver.v1.types.Point3D;

import static java.time.ZoneOffset.UTC;
import static org.neo4j.driver.v1.Values.isoDuration;
import static org.neo4j.driver.v1.Values.point2D;
import static org.neo4j.driver.v1.Values.point3D;
import static org.neo4j.driver.v1.Values.value;

public class PackStreamMessageFormatV2 extends PackStreamMessageFormatV1
{
    private static final byte DATE = 'D';
    private static final int DATE_STRUCT_SIZE = 1;

    private static final byte TIME = 'T';
    private static final int TIME_STRUCT_SIZE = 2;

    private static final byte LOCAL_TIME = 't';
    private static final int LOCAL_TIME_STRUCT_SIZE = 1;

    private static final byte LOCAL_DATE_TIME = 'd';
    private static final int LOCAL_DATE_TIME_STRUCT_SIZE = 2;

    private static final byte DATE_TIME_WITH_ZONE_OFFSET = 'F';
    private static final byte DATE_TIME_WITH_ZONE_ID = 'f';
    private static final int DATE_TIME_STRUCT_SIZE = 3;

    private static final byte DURATION = 'E';
    private static final int DURATION_TIME_STRUCT_SIZE = 4;

    private static final byte POINT_2D_STRUCT_TYPE = 'X';
    private static final int POINT_2D_STRUCT_SIZE = 3;

    private static final byte POINT_3D_STRUCT_TYPE = 'Y';
    private static final int POINT_3D_STRUCT_SIZE = 4;

    @Override
    public MessageFormat.Writer newWriter( PackOutput output, boolean byteArraySupportEnabled )
    {
        if ( !byteArraySupportEnabled )
        {
            throw new IllegalArgumentException( "Bolt V2 should support byte arrays" );
        }
        return new WriterV2( output );
    }

    @Override
    public MessageFormat.Reader newReader( PackInput input )
    {
        return new ReaderV2( input );
    }

    private static class WriterV2 extends WriterV1
    {
        WriterV2( PackOutput output )
        {
            super( output, true );
        }

        @Override
        void packInternalValue( InternalValue value ) throws IOException
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
            case POINT_2D:
                packPoint2D( value.asPoint2D() );
                break;
            case POINT_3D:
                packPoint3D( value.asPoint3D() );
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
            OffsetTime offsetTimeUtc = offsetTime.withOffsetSameInstant( UTC );
            long nanoOfDayUtc = offsetTimeUtc.toLocalTime().toNanoOfDay();
            int offsetSeconds = offsetTime.getOffset().getTotalSeconds();

            packer.packStructHeader( TIME_STRUCT_SIZE, TIME );
            packer.pack( nanoOfDayUtc );
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
            Instant instant = zonedDateTime.toInstant();
            ZoneId zone = zonedDateTime.getZone();

            if ( zone instanceof ZoneOffset )
            {
                int offsetSeconds = ((ZoneOffset) zone).getTotalSeconds();

                packer.packStructHeader( DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_OFFSET );
                packer.pack( instant.getEpochSecond() );
                packer.pack( instant.getNano() );
                packer.pack( offsetSeconds );
            }
            else
            {
                String zoneId = zone.getId();

                packer.packStructHeader( DATE_TIME_STRUCT_SIZE, DATE_TIME_WITH_ZONE_ID );
                packer.pack( instant.getEpochSecond() );
                packer.pack( instant.getNano() );
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

        private void packPoint2D( Point2D point ) throws IOException
        {
            packer.packStructHeader( POINT_2D_STRUCT_SIZE, POINT_2D_STRUCT_TYPE );
            packer.pack( point.srid() );
            packer.pack( point.x() );
            packer.pack( point.y() );
        }

        private void packPoint3D( Point3D point ) throws IOException
        {
            packer.packStructHeader( POINT_3D_STRUCT_SIZE, POINT_3D_STRUCT_TYPE );
            packer.pack( point.srid() );
            packer.pack( point.x() );
            packer.pack( point.y() );
            packer.pack( point.z() );
        }
    }

    private static class ReaderV2 extends ReaderV1
    {
        ReaderV2( PackInput input )
        {
            super( input );
        }

        @Override
        Value unpackStruct( long size, byte type ) throws IOException
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
                ensureCorrectStructSize( TypeConstructor.POINT_2D, POINT_2D_STRUCT_SIZE, size );
                return unpackPoint2D();
            case POINT_3D_STRUCT_TYPE:
                ensureCorrectStructSize( TypeConstructor.POINT_3D, POINT_3D_STRUCT_SIZE, size );
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
            long nanoOfDayUtc = unpacker.unpackLong();
            int offsetSeconds = Math.toIntExact( unpacker.unpackLong() );

            Instant instant = Instant.ofEpochSecond( 0, nanoOfDayUtc );
            ZoneOffset offset = ZoneOffset.ofTotalSeconds( offsetSeconds );
            return value( OffsetTime.ofInstant( instant, offset ) );
        }

        private Value unpackLocalTime() throws IOException
        {
            long nanoOfDay = unpacker.unpackLong();
            return value( LocalTime.ofNanoOfDay( nanoOfDay ) );
        }

        private Value unpackLocalDateTime() throws IOException
        {
            long epochSecondUtc = unpacker.unpackLong();
            int nano = Math.toIntExact( unpacker.unpackLong() );
            return value( LocalDateTime.ofEpochSecond( epochSecondUtc, nano, UTC ) );
        }

        private Value unpackDateTimeWithZoneOffset() throws IOException
        {
            long epochSecondUtc = unpacker.unpackLong();
            int nano = Math.toIntExact( unpacker.unpackLong() );
            int offsetSeconds = Math.toIntExact( unpacker.unpackLong() );

            Instant instant = Instant.ofEpochSecond( epochSecondUtc, nano );
            ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds( offsetSeconds );
            return value( ZonedDateTime.ofInstant( instant, zoneOffset ) );
        }

        private Value unpackDateTimeWithZoneId() throws IOException
        {
            long epochSecondUtc = unpacker.unpackLong();
            int nano = Math.toIntExact( unpacker.unpackLong() );
            String zoneIdString = unpacker.unpackString();

            Instant instant = Instant.ofEpochSecond( epochSecondUtc, nano );
            ZoneId zoneId = ZoneId.of( zoneIdString );
            return value( ZonedDateTime.ofInstant( instant, zoneId ) );
        }

        private Value unpackDuration() throws IOException
        {
            long months = unpacker.unpackLong();
            long days = unpacker.unpackLong();
            long seconds = unpacker.unpackLong();
            long nanoseconds = unpacker.unpackLong();
            return isoDuration( months, days, seconds, nanoseconds );
        }

        private Value unpackPoint2D() throws IOException
        {
            long srid = unpacker.unpackLong();
            double x = unpacker.unpackDouble();
            double y = unpacker.unpackDouble();
            return point2D( srid, x, y );
        }

        private Value unpackPoint3D() throws IOException
        {
            long srid = unpacker.unpackLong();
            double x = unpacker.unpackDouble();
            double y = unpacker.unpackDouble();
            double z = unpacker.unpackDouble();
            return point3D( srid, x, y, z );
        }
    }
}
