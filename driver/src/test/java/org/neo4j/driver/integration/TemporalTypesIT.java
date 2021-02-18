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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.IsoDuration;
import java.util.function.Function;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;
import org.neo4j.driver.util.TemporalUtil;

import static java.time.Month.MARCH;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.internal.util.Neo4jFeature.TEMPORAL_TYPES;
import static org.neo4j.driver.Values.isoDuration;
import static org.neo4j.driver.Values.ofOffsetDateTime;
import static org.neo4j.driver.Values.parameters;

@EnabledOnNeo4jWith( TEMPORAL_TYPES )
@ParallelizableIT
class TemporalTypesIT
{
    private static final int RANDOM_VALUES_TO_TEST = 1_000;

    private static final int RANDOM_LISTS_TO_TEST = 100;
    private static final int MIN_LIST_SIZE = 100;
    private static final int MAX_LIST_SIZE = 1_000;

    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldSendDate()
    {
        testSendValue( LocalDate.now(), Value::asLocalDate );
    }

    @Test
    void shouldReceiveDate()
    {
        testReceiveValue( "RETURN date({year: 1995, month: 12, day: 4})",
                LocalDate.of( 1995, 12, 4 ),
                Value::asLocalDate );
    }

    @Test
    void shouldSendAndReceiveDate()
    {
        testSendAndReceiveValue( LocalDate.now(), Value::asLocalDate );
    }

    @Test
    void shouldSendAndReceiveRandomDate()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomLocalDate, Value::asLocalDate );
    }

    @Test
    void shouldSendAndReceiveListsWithRandomDates()
    {
        testSendAndReceiveRandomLists( TemporalUtil::randomLocalDate );
    }

    @Test
    void shouldSendTime()
    {
        testSendValue( OffsetTime.now(), Value::asOffsetTime );
    }

    @Test
    void shouldReceiveTime()
    {
        testReceiveValue( "RETURN time({hour: 23, minute: 19, second: 55, timezone:'-07:00'})",
                OffsetTime.of( 23, 19, 55, 0, ZoneOffset.ofHours( -7 ) ),
                Value::asOffsetTime );
    }

    @Test
    void shouldSendAndReceiveTime()
    {
        testSendAndReceiveValue( OffsetTime.now(), Value::asOffsetTime );
    }

    @Test
    void shouldSendAndReceiveRandomTime()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomOffsetTime, Value::asOffsetTime );
    }

    @Test
    void shouldSendAndReceiveListsWithRandomTimes()
    {
        testSendAndReceiveRandomLists( TemporalUtil::randomOffsetTime );
    }

    @Test
    void shouldSendLocalTime()
    {
        testSendValue( LocalTime.now(), Value::asLocalTime );
    }

    @Test
    void shouldReceiveLocalTime()
    {
        testReceiveValue( "RETURN localtime({hour: 22, minute: 59, second: 10, nanosecond: 999999})",
                LocalTime.of( 22, 59, 10, 999_999 ),
                Value::asLocalTime );
    }

    @Test
    void shouldSendAndReceiveLocalTime()
    {
        testSendAndReceiveValue( LocalTime.now(), Value::asLocalTime );
    }

    @Test
    void shouldSendAndReceiveRandomLocalTime()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomLocalTime, Value::asLocalTime );
    }

    @Test
    void shouldSendAndReceiveListsWithRandomLocalTimes()
    {
        testSendAndReceiveRandomLists( TemporalUtil::randomLocalTime );
    }

    @Test
    void shouldSendLocalDateTime()
    {
        testSendValue( LocalDateTime.now(), Value::asLocalDateTime );
    }

    @Test
    void shouldReceiveLocalDateTime()
    {
        testReceiveValue( "RETURN localdatetime({year: 1899, month: 3, day: 20, hour: 12, minute: 17, second: 13, nanosecond: 999})",
                LocalDateTime.of( 1899, MARCH, 20, 12, 17, 13, 999 ),
                Value::asLocalDateTime );
    }

    @Test
    void shouldSendAndReceiveLocalDateTime()
    {
        testSendAndReceiveValue( LocalDateTime.now(), Value::asLocalDateTime );
    }

    @Test
    void shouldSendAndReceiveRandomLocalDateTime()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomLocalDateTime, Value::asLocalDateTime );
    }

    @Test
    void shouldSendAndReceiveListsWithRandomLocalDateTimes()
    {
        testSendAndReceiveRandomLists( TemporalUtil::randomLocalDateTime );
    }

    @Test
    void shouldSendDateTimeWithZoneOffset()
    {
        ZoneOffset offset = ZoneOffset.ofHoursMinutes( -4, -15 );
        testSendValue( ZonedDateTime.of( 1845, 3, 25, 19, 15, 45, 22, offset ), Value::asZonedDateTime );
    }

    @Test
    void shouldReceiveDateTimeWithZoneOffset()
    {
        ZoneOffset offset = ZoneOffset.ofHoursMinutes( 3, 30 );
        testReceiveValue( "RETURN datetime({year:1984, month:10, day:11, hour:21, minute:30, second:34, timezone:'+03:30'})",
                ZonedDateTime.of( 1984, 10, 11, 21, 30, 34, 0, offset ),
                Value::asZonedDateTime );
    }

    @Test
    void shouldSendAndReceiveDateTimeWithZoneOffset()
    {
        ZoneOffset offset = ZoneOffset.ofHoursMinutes( -7, -15 );
        testSendAndReceiveValue( ZonedDateTime.of( 2017, 3, 9, 11, 12, 13, 14, offset ), Value::asZonedDateTime );
    }

    @Test
    void shouldSendAndReceiveRandomDateTimeWithZoneOffset()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomZonedDateTimeWithOffset, Value::asZonedDateTime );
    }

    @Test
    void shouldSendAndReceiveListsWithRandomDateTimeWithZoneOffsets()
    {
        testSendAndReceiveRandomLists( TemporalUtil::randomZonedDateTimeWithOffset );
    }

    @Test
    void shouldSendDateTimeRepresentedWithOffsetDateTime()
    {
        testSendValue( OffsetDateTime.of( 1851, 9, 29, 1, 29, 42, 987, ZoneOffset.ofHours( -8 ) ), Value::asOffsetDateTime );
    }

    @Test
    void shouldReceiveDateTimeRepresentedWithOffsetDateTime()
    {
        testReceiveValue( "RETURN datetime({year:2121, month:1, day:1, hour:2, minute:2, second:2, timezone:'-07:20'})",
                OffsetDateTime.of( 2121, 1, 1, 2, 2, 2, 0, ZoneOffset.ofHoursMinutes( -7, -20 ) ),
                Value::asOffsetDateTime );
    }

    @Test
    void shouldSendAndReceiveDateTimeRepresentedWithOffsetDateTime()
    {
        testSendAndReceiveValue( OffsetDateTime.of( 1998, 12, 12, 23, 54, 14, 123, ZoneOffset.ofHoursMinutes( 1, 15 ) ), Value::asOffsetDateTime );
    }

    @Test
    void shouldSendAndReceiveRandomDateTimeRepresentedWithOffsetDateTime()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomOffsetDateTime, Value::asOffsetDateTime );
    }

    @Test
    void shouldSendAndReceiveListsWithRandomDateTimeRepresentedWithOffsetDateTimes()
    {
        testSendAndReceiveRandomLists( TemporalUtil::randomOffsetDateTime, value -> value.asList( ofOffsetDateTime() ) );
    }

    @Test
    void shouldSendDateTimeWithZoneId()
    {
        ZoneId zoneId = ZoneId.of( "Europe/Stockholm" );
        testSendValue( ZonedDateTime.of( 2049, 9, 11, 19, 10, 40, 20, zoneId ), Value::asZonedDateTime );
    }

    @Test
    void shouldReceiveDateTimeWithZoneId()
    {
        ZoneId zoneId = ZoneId.of( "Europe/London" );
        testReceiveValue( "RETURN datetime({year:2000, month:1, day:1, hour:9, minute:5, second:1, timezone:'Europe/London'})",
                ZonedDateTime.of( 2000, 1, 1, 9, 5, 1, 0, zoneId ),
                Value::asZonedDateTime );
    }

    @Test
    void shouldSendAndReceiveDateTimeWithZoneId()
    {
        ZoneId zoneId = ZoneId.of( "Europe/Stockholm" );
        testSendAndReceiveValue( ZonedDateTime.of( 2099, 12, 29, 12, 59, 59, 59, zoneId ), Value::asZonedDateTime );
    }

    @Test
    void shouldSendAndReceiveRandomDateTimeWithZoneId()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomZonedDateTimeWithZoneId, Value::asZonedDateTime );
    }

    @Test
    void shouldSendAndReceiveListsWithRandomDateTimeWithZoneIds()
    {
        testSendAndReceiveRandomLists( TemporalUtil::randomZonedDateTimeWithZoneId );
    }

    @Test
    void shouldSendDuration()
    {
        testSendValue( newDuration( 8, 12, 90, 8 ), Value::asIsoDuration );
    }

    @Test
    void shouldReceiveDuration()
    {
        testReceiveValue( "RETURN duration({months: 13, days: 40, seconds: 12, nanoseconds: 999})",
                newDuration( 13, 40, 12, 999 ),
                Value::asIsoDuration );
    }

    @Test
    void shouldSendAndReceiveDuration()
    {
        testSendAndReceiveValue( newDuration( 7, 7, 88, 999_999 ), Value::asIsoDuration );
    }

    @Test
    void shouldSendAndReceiveRandomDuration()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomDuration, Value::asIsoDuration );
    }

    @Test
    void shouldSendAndReceiveListsWithRandomDurations()
    {
        testSendAndReceiveRandomLists( TemporalUtil::randomDuration );
    }

    @Test
    void shouldFormatDurationToString()
    {
        testDurationToString( 1, 0, "P0M0DT1S" );
        testDurationToString( -1, 0, "P0M0DT-1S" );

        testDurationToString( 0, 5, "P0M0DT0.000000005S" );
        testDurationToString( 0, -5, "P0M0DT-0.000000005S" );
        testDurationToString( 0, 999_999_999, "P0M0DT0.999999999S" );
        testDurationToString( 0, -999_999_999, "P0M0DT-0.999999999S" );

        testDurationToString( 1, 5, "P0M0DT1.000000005S" );
        testDurationToString( -1, -5, "P0M0DT-1.000000005S" );
        testDurationToString( 1, -5, "P0M0DT0.999999995S" );
        testDurationToString( -1, 5, "P0M0DT-0.999999995S" );
        testDurationToString( 1, 999999999, "P0M0DT1.999999999S" );
        testDurationToString( -1, -999999999, "P0M0DT-1.999999999S" );
        testDurationToString( 1, -999999999, "P0M0DT0.000000001S" );
        testDurationToString( -1, 999999999, "P0M0DT-0.000000001S" );

        testDurationToString( -78036, -143000000, "P0M0DT-78036.143000000S" );

        testDurationToString( 0, 1_000_000_000, "P0M0DT1S" );
        testDurationToString( 0, -1_000_000_000, "P0M0DT-1S" );
        testDurationToString( 0, 1_000_000_007, "P0M0DT1.000000007S" );
        testDurationToString( 0, -1_000_000_007, "P0M0DT-1.000000007S" );

        testDurationToString( 40, 2_123_456_789, "P0M0DT42.123456789S" );
        testDurationToString( -40, 2_123_456_789, "P0M0DT-37.876543211S" );
        testDurationToString( 40, -2_123_456_789, "P0M0DT37.876543211S" );
        testDurationToString( -40, -2_123_456_789, "P0M0DT-42.123456789S" );
    }

    private static <T> void testSendAndReceiveRandomValues( Supplier<T> valueSupplier, Function<Value,T> converter )
    {
        for ( int i = 0; i < RANDOM_VALUES_TO_TEST; i++ )
        {
            testSendAndReceiveValue( valueSupplier.get(), converter );
        }
    }

    private static <T> void testSendAndReceiveRandomLists( Supplier<T> valueSupplier )
    {
        testSendAndReceiveRandomLists( valueSupplier::get, Value::asList );
    }

    private static <T> void testSendAndReceiveRandomLists( Supplier<T> valueSupplier, Function<Value,List<T>> converter )
    {
        for ( int i = 0; i < RANDOM_LISTS_TO_TEST; i++ )
        {
            int listSize = ThreadLocalRandom.current().nextInt( MIN_LIST_SIZE, MAX_LIST_SIZE );
            List<T> list = Stream.generate( valueSupplier )
                    .limit( listSize )
                    .collect( toList() );

            testSendAndReceiveValue( list, converter );
        }
    }

    private static <T> void testSendValue( T value, Function<Value,T> converter )
    {
        Record record1 = session.run( "CREATE (n:Node {value: $value}) RETURN 42", singletonMap( "value", value ) ).single();
        assertEquals( 42, record1.get( 0 ).asInt() );

        Record record2 = session.run( "MATCH (n:Node) RETURN n.value" ).single();
        assertEquals( value, converter.apply( record2.get( 0 ) ) );
    }

    private static <T> void testReceiveValue( String query, T expectedValue, Function<Value,T> converter )
    {
        Record record = session.run( query ).single();
        assertEquals( expectedValue, converter.apply( record.get( 0 ) ) );
    }

    private static <T> void testSendAndReceiveValue( T value, Function<Value,T> converter )
    {
        Record record = session.run( "CREATE (n:Node {value: $value}) RETURN n.value", singletonMap( "value", value ) ).single();
        assertEquals( value, converter.apply( record.get( 0 ) ) );
    }

    private static void testDurationToString( long seconds, int nanoseconds, String expectedValue )
    {
        Result result = session.run( "RETURN duration({seconds: $s, nanoseconds: $n})", parameters( "s", seconds, "n", nanoseconds ) );
        IsoDuration duration = result.single().get( 0 ).asIsoDuration();
        assertEquals( expectedValue, duration.toString() );
    }

    private static IsoDuration newDuration( long months, long days, long seconds, int nanoseconds )
    {
        return isoDuration( months, days, seconds, nanoseconds ).asIsoDuration();
    }
}
