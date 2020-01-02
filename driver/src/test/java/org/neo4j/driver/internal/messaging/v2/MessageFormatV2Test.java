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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.async.inbound.ByteBufInput;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.util.ThrowingConsumer;
import org.neo4j.driver.internal.util.io.ByteBufOutput;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;

import static java.time.Month.APRIL;
import static java.time.Month.AUGUST;
import static java.time.Month.DECEMBER;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.packstream.PackStream.FLOAT_64;
import static org.neo4j.driver.internal.packstream.PackStream.INT_16;
import static org.neo4j.driver.internal.packstream.PackStream.INT_32;
import static org.neo4j.driver.internal.packstream.PackStream.INT_64;
import static org.neo4j.driver.internal.packstream.PackStream.Packer;
import static org.neo4j.driver.internal.packstream.PackStream.STRING_8;
import static org.neo4j.driver.util.TestUtil.assertByteBufContains;

class MessageFormatV2Test
{
    private final MessageFormatV2 messageFormat = new MessageFormatV2();

    @Test
    void shouldWritePoint2D() throws Exception
    {
        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = newWriter( buf );

        writer.write( new RunMessage( "RETURN $point", singletonMap( "point", point( 42, 12.99, -180.0 ) ) ) );

        // point should be encoded as (byte SRID, FLOAT_64 header byte + double X, FLOAT_64 header byte + double Y)
        int index = buf.readableBytes() - Double.BYTES - Byte.BYTES - Double.BYTES - Byte.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice, (byte) 42, FLOAT_64, 12.99, FLOAT_64, -180.0 );
    }

    @Test
    void shouldWritePoint3D() throws Exception
    {
        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = newWriter( buf );

        writer.write( new RunMessage( "RETURN $point", singletonMap( "point", point( 42, 0.51, 2.99, 100.123 ) ) ) );

        // point should be encoded as (byte SRID, FLOAT_64 header byte + double X, FLOAT_64 header byte + double Y, FLOAT_64 header byte + double Z)
        int index = buf.readableBytes() - Double.BYTES - Byte.BYTES - Double.BYTES - Byte.BYTES - Double.BYTES - Byte.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice, (byte) 42, FLOAT_64, 0.51, FLOAT_64, 2.99, FLOAT_64, 100.123 );
    }

    @Test
    void shouldReadPoint2D() throws Exception
    {
        Point point = new InternalPoint2D( 42, 120.65, -99.2 );

        Object unpacked = packAndUnpackValue( packer ->
        {
            packer.packStructHeader( 3, (byte) 'X' );
            packer.pack( point.srid() );
            packer.pack( point.x() );
            packer.pack( point.y() );
        } );

        assertEquals( point, unpacked );
    }

    @Test
    void shouldReadPoint3D() throws Exception
    {
        Point point = new InternalPoint3D( 42, 85.391, 98.8, 11.1 );

        Object unpacked = packAndUnpackValue( packer ->
        {
            packer.packStructHeader( 4, (byte) 'Y' );
            packer.pack( point.srid() );
            packer.pack( point.x() );
            packer.pack( point.y() );
            packer.pack( point.z() );
        } );

        assertEquals( point, unpacked );
    }

    @Test
    void shouldWriteDate() throws Exception
    {
        LocalDate date = LocalDate.ofEpochDay( 2147483650L );
        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = newWriter( buf );

        writer.write( new RunMessage( "RETURN $date", singletonMap( "date", value( date ) ) ) );

        int index = buf.readableBytes() - Long.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice, INT_64, date.toEpochDay() );
    }

    @Test
    void shouldReadDate() throws Exception
    {
        LocalDate date = LocalDate.of( 2012, AUGUST, 3 );

        Object unpacked = packAndUnpackValue( packer ->
        {
            packer.packStructHeader( 1, (byte) 'D' );
            packer.pack( date.toEpochDay() );
        } );

        assertEquals( date, unpacked );
    }

    @Test
    void shouldWriteTime() throws Exception
    {
        OffsetTime time = OffsetTime.of( 4, 16, 20, 999, ZoneOffset.MIN );
        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = newWriter( buf );

        writer.write( new RunMessage( "RETURN $time", singletonMap( "time", value( time ) ) ) );

        int index = buf.readableBytes() - Long.BYTES - Byte.BYTES - Integer.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice, INT_64, time.toLocalTime().toNanoOfDay(), INT_32, time.getOffset().getTotalSeconds() );
    }

    @Test
    void shouldReadTime() throws Exception
    {
        OffsetTime time = OffsetTime.of( 23, 59, 59, 999, ZoneOffset.MAX );

        Object unpacked = packAndUnpackValue( packer ->
        {
            packer.packStructHeader( 2, (byte) 'T' );
            packer.pack( time.toLocalTime().toNanoOfDay() );
            packer.pack( time.getOffset().getTotalSeconds() );
        } );

        assertEquals( time, unpacked );
    }

    @Test
    void shouldWriteLocalTime() throws Exception
    {
        LocalTime time = LocalTime.of( 12, 9, 18, 999_888 );
        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = newWriter( buf );

        writer.write( new RunMessage( "RETURN $time", singletonMap( "time", value( time ) ) ) );

        int index = buf.readableBytes() - Long.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice, INT_64, time.toNanoOfDay() );
    }

    @Test
    void shouldReadLocalTime() throws Exception
    {
        LocalTime time = LocalTime.of( 12, 25 );

        Object unpacked = packAndUnpackValue( packer ->
        {
            packer.packStructHeader( 1, (byte) 't' );
            packer.pack( time.toNanoOfDay() );
        } );

        assertEquals( time, unpacked );
    }

    @Test
    void shouldWriteLocalDateTime() throws Exception
    {
        LocalDateTime dateTime = LocalDateTime.of( 2049, DECEMBER, 12, 17, 25, 49, 199 );
        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = newWriter( buf );

        writer.write( new RunMessage( "RETURN $dateTime", singletonMap( "dateTime", value( dateTime ) ) ) );

        int index = buf.readableBytes() - Long.BYTES - Byte.BYTES - Short.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice, INT_64, dateTime.toEpochSecond( UTC ), INT_16, (short) dateTime.getNano() );
    }

    @Test
    void shouldReadLocalDateTime() throws Exception
    {
        LocalDateTime dateTime = LocalDateTime.of( 1999, APRIL, 3, 19, 5, 5, 100_200_300 );

        Object unpacked = packAndUnpackValue( packer ->
        {
            packer.packStructHeader( 2, (byte) 'd' );
            packer.pack( dateTime.toEpochSecond( UTC ) );
            packer.pack( dateTime.getNano() );
        } );

        assertEquals( dateTime, unpacked );
    }

    @Test
    void shouldWriteZonedDateTimeWithOffset() throws Exception
    {
        ZoneOffset zoneOffset = ZoneOffset.ofHoursMinutes( 9, 30 );
        ZonedDateTime dateTime = ZonedDateTime.of( 2000, 1, 10, 12, 2, 49, 300, zoneOffset );
        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = newWriter( buf );

        writer.write( new RunMessage( "RETURN $dateTime", singletonMap( "dateTime", value( dateTime ) ) ) );

        int index = buf.readableBytes() - Integer.BYTES - Byte.BYTES - Short.BYTES - Byte.BYTES - Integer.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice,
                INT_32, (int) localEpochSecondOf( dateTime ),
                INT_16, (short) dateTime.getNano(),
                INT_32, zoneOffset.getTotalSeconds() );
    }

    @Test
    void shouldReadZonedDateTimeWithOffset() throws Exception
    {
        ZoneOffset zoneOffset = ZoneOffset.ofHoursMinutes( -7, -15 );
        ZonedDateTime dateTime = ZonedDateTime.of( 1823, 1, 12, 23, 59, 59, 999_999_999, zoneOffset );

        Object unpacked = packAndUnpackValue( packer ->
        {
            packer.packStructHeader( 3, (byte) 'F' );
            packer.pack( localEpochSecondOf( dateTime ) );
            packer.pack( dateTime.toInstant().getNano() );
            packer.pack( zoneOffset.getTotalSeconds() );
        } );

        assertEquals( dateTime, unpacked );
    }

    @Test
    void shouldWriteZonedDateTimeWithZoneId() throws Exception
    {
        String zoneName = "Europe/Stockholm";
        byte[] zoneNameBytes = zoneName.getBytes();
        ZonedDateTime dateTime = ZonedDateTime.of( 2000, 1, 10, 12, 2, 49, 300, ZoneId.of( zoneName ) );

        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = newWriter( buf );

        writer.write( new RunMessage( "RETURN $dateTime", singletonMap( "dateTime", value( dateTime ) ) ) );

        int index = buf.readableBytes() - zoneNameBytes.length - Byte.BYTES - Byte.BYTES - Short.BYTES - Byte.BYTES - Integer.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        List<Number> expectedBuf = new ArrayList<>( asList(
                INT_32, (int) localEpochSecondOf( dateTime ),
                INT_16, (short) dateTime.toInstant().getNano(),
                STRING_8, (byte) zoneNameBytes.length ) );

        for ( byte b : zoneNameBytes )
        {
            expectedBuf.add( b );
        }

        assertByteBufContains( tailSlice, expectedBuf.toArray( new Number[0] ) );
    }

    @Test
    void shouldReadZonedDateTimeWithZoneId() throws Exception
    {
        String zoneName = "Europe/Stockholm";
        ZonedDateTime dateTime = ZonedDateTime.of( 1823, 1, 12, 23, 59, 59, 999_999_999, ZoneId.of( zoneName ) );

        Object unpacked = packAndUnpackValue( packer ->
        {
            packer.packStructHeader( 3, (byte) 'f' );
            packer.pack( localEpochSecondOf( dateTime ) );
            packer.pack( dateTime.toInstant().getNano() );
            packer.pack( zoneName );
        } );

        assertEquals( dateTime, unpacked );
    }

    @Test
    void shouldWriteDuration() throws Exception
    {
        Value durationValue = Values.isoDuration( Long.MAX_VALUE - 1, Integer.MAX_VALUE - 1, Short.MAX_VALUE - 1, Byte.MAX_VALUE - 1 );
        IsoDuration duration = durationValue.asIsoDuration();

        ByteBuf buf = Unpooled.buffer();
        MessageFormat.Writer writer = newWriter( buf );

        writer.write( new RunMessage( "RETURN $duration", singletonMap( "duration", durationValue ) ) );

        int index = buf.readableBytes() - Long.BYTES - Byte.BYTES - Integer.BYTES - Byte.BYTES - Short.BYTES - Byte.BYTES - Byte.BYTES;
        ByteBuf tailSlice = buf.slice( index, buf.readableBytes() - index );

        assertByteBufContains( tailSlice,
                INT_64, duration.months(), INT_32, (int) duration.days(), INT_16, (short) duration.seconds(), (byte) duration.nanoseconds() );
    }

    @Test
    void shouldReadDuration() throws Exception
    {
        Value durationValue = Values.isoDuration( 17, 22, 99, 15 );
        IsoDuration duration = durationValue.asIsoDuration();

        Object unpacked = packAndUnpackValue( packer ->
        {
            packer.packStructHeader( 4, (byte) 'E' );
            packer.pack( duration.months() );
            packer.pack( duration.days() );
            packer.pack( duration.seconds() );
            packer.pack( duration.nanoseconds() );
        } );

        assertEquals( duration, unpacked );
    }

    private Object packAndUnpackValue( ThrowingConsumer<Packer> packAction ) throws Exception
    {
        ByteBuf buf = Unpooled.buffer();
        try
        {
            Packer packer = new Packer( new ByteBufOutput( buf ) );
            packer.packStructHeader( 1, RecordMessage.SIGNATURE );
            packer.packListHeader( 1 );
            packAction.accept( packer );

            ByteBufInput input = new ByteBufInput();
            input.start( buf );
            MessageFormat.Reader reader = messageFormat.newReader( input );

            List<Value> values = new ArrayList<>();
            ResponseMessageHandler responseHandler = recordMemorizingHandler( values );
            reader.read( responseHandler );
            input.stop();

            assertEquals( 1, values.size() );
            return values.get( 0 ).asObject();
        }
        finally
        {
            buf.release();
        }
    }

    private static ResponseMessageHandler recordMemorizingHandler( List<Value> values ) throws IOException
    {
        ResponseMessageHandler responseHandler = mock( ResponseMessageHandler.class );
        doAnswer( invocation ->
        {
            Value[] arg = invocation.getArgument( 0 );
            Collections.addAll( values, arg );
            return null;
        } ).when( responseHandler ).handleRecordMessage( any() );
        return responseHandler;
    }

    private MessageFormat.Writer newWriter( ByteBuf buf )
    {
        return messageFormat.newWriter( new ByteBufOutput( buf ) );
    }

    private static long localEpochSecondOf( ZonedDateTime dateTime )
    {
        return dateTime.toLocalDateTime().toEpochSecond( UTC );
    }
}
