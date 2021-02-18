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
package org.neo4j.driver.internal.messaging.common;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;

import static java.time.ZoneOffset.UTC;

public class CommonValuePacker implements ValuePacker
{

    public static final byte DATE = 'D';
    public static final int DATE_STRUCT_SIZE = 1;

    public static final byte TIME = 'T';
    public static final int TIME_STRUCT_SIZE = 2;

    public static final byte LOCAL_TIME = 't';
    public static final int LOCAL_TIME_STRUCT_SIZE = 1;

    public static final byte LOCAL_DATE_TIME = 'd';
    public static final int LOCAL_DATE_TIME_STRUCT_SIZE = 2;

    public static final byte DATE_TIME_WITH_ZONE_OFFSET = 'F';
    public static final byte DATE_TIME_WITH_ZONE_ID = 'f';
    public static final int DATE_TIME_STRUCT_SIZE = 3;

    public static final byte DURATION = 'E';
    public static final int DURATION_TIME_STRUCT_SIZE = 4;

    public static final byte POINT_2D_STRUCT_TYPE = 'X';
    public static final int POINT_2D_STRUCT_SIZE = 3;

    public static final byte POINT_3D_STRUCT_TYPE = 'Y';
    public static final int POINT_3D_STRUCT_SIZE = 4;

    protected final PackStream.Packer packer;

    public CommonValuePacker( PackOutput output )
    {
        this.packer = new PackStream.Packer( output );
    }

    @Override
    public final void packStructHeader( int size, byte signature ) throws IOException
    {
        packer.packStructHeader( size, signature );
    }

    @Override
    public final void pack( String string ) throws IOException
    {
        packer.pack( string );
    }

    @Override
    public final void pack( Value value ) throws IOException
    {
        if ( value instanceof InternalValue )
        {
            packInternalValue( ((InternalValue) value) );
        }
        else
        {
            throw new IllegalArgumentException( "Unable to pack: " + value );
        }
    }

    @Override
    public final void pack( Map<String,Value> map ) throws IOException
    {
        if ( map == null || map.size() == 0 )
        {
            packer.packMapHeader( 0 );
            return;
        }
        packer.packMapHeader( map.size() );
        for ( Map.Entry<String,Value> entry : map.entrySet() )
        {
            packer.pack( entry.getKey() );
            pack( entry.getValue() );
        }
    }

    protected void packInternalValue( InternalValue value ) throws IOException
    {
        switch ( value.typeConstructor() )
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
        case NULL:
            packer.packNull();
            break;

        case BYTES:
            packer.pack( value.asByteArray() );
            break;

        case STRING:
            packer.pack( value.asString() );
            break;

        case BOOLEAN:
            packer.pack( value.asBoolean() );
            break;

        case INTEGER:
            packer.pack( value.asLong() );
            break;

        case FLOAT:
            packer.pack( value.asDouble() );
            break;

        case MAP:
            packer.packMapHeader( value.size() );
            for ( String s : value.keys() )
            {
                packer.pack( s );
                pack( value.get( s ) );
            }
            break;

        case LIST:
            packer.packListHeader( value.size() );
            for ( Value item : value.values() )
            {
                pack( item );
            }
            break;

        default:
            throw new IOException( "Unknown type: " + value.type().name() );
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
