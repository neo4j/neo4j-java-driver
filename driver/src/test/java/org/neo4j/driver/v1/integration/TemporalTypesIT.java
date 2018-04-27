/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.v1.integration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Supplier;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.IsoDuration;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.TemporalUtil;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static java.time.Month.MARCH;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.internal.util.ServerVersion.v3_4_0;
import static org.neo4j.driver.v1.Values.isoDuration;

public class TemporalTypesIT
{
    private static final int RANDOM_VALUES_TO_TEST = 1_000;

    @Rule
    public final TestNeo4jSession session = new TestNeo4jSession();

    @Before
    public void setUp()
    {
        assumeTrue( session.version().greaterThanOrEqual( v3_4_0 ) );
    }

    @Test
    public void shouldSendDate()
    {
        testSendValue( LocalDate.now(), Value::asLocalDate );
    }

    @Test
    public void shouldReceiveDate()
    {
        testReceiveValue( "RETURN date({year: 1995, month: 12, day: 4})",
                LocalDate.of( 1995, 12, 4 ),
                Value::asLocalDate );
    }

    @Test
    public void shouldSendAndReceiveDate()
    {
        testSendAndReceiveValue( LocalDate.now(), Value::asLocalDate );
    }

    @Test
    public void shouldSendAndReceiveRandomDate()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomLocalDate, Value::asLocalDate );
    }

    @Test
    public void shouldSendTime()
    {
        testSendValue( OffsetTime.now(), Value::asOffsetTime );
    }

    @Test
    public void shouldReceiveTime()
    {
        testReceiveValue( "RETURN time({hour: 23, minute: 19, second: 55, timezone:'-07:00'})",
                OffsetTime.of( 23, 19, 55, 0, ZoneOffset.ofHours( -7 ) ),
                Value::asOffsetTime );
    }

    @Test
    public void shouldSendAndReceiveTime()
    {
        testSendAndReceiveValue( OffsetTime.now(), Value::asOffsetTime );
    }

    @Test
    public void shouldSendAndReceiveRandomTime()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomOffsetTime, Value::asOffsetTime );
    }

    @Test
    public void shouldSendLocalTime()
    {
        testSendValue( LocalTime.now(), Value::asLocalTime );
    }

    @Test
    public void shouldReceiveLocalTime()
    {
        testReceiveValue( "RETURN localtime({hour: 22, minute: 59, second: 10, nanosecond: 999999})",
                LocalTime.of( 22, 59, 10, 999_999 ),
                Value::asLocalTime );
    }

    @Test
    public void shouldSendAndReceiveLocalTime()
    {
        testSendAndReceiveValue( LocalTime.now(), Value::asLocalTime );
    }

    @Test
    public void shouldSendAndReceiveRandomLocalTime()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomLocalTime, Value::asLocalTime );
    }

    @Test
    public void shouldSendLocalDateTime()
    {
        testSendValue( LocalDateTime.now(), Value::asLocalDateTime );
    }

    @Test
    public void shouldReceiveLocalDateTime()
    {
        testReceiveValue( "RETURN localdatetime({year: 1899, month: 3, day: 20, hour: 12, minute: 17, second: 13, nanosecond: 999})",
                LocalDateTime.of( 1899, MARCH, 20, 12, 17, 13, 999 ),
                Value::asLocalDateTime );
    }

    @Test
    public void shouldSendAndReceiveLocalDateTime()
    {
        testSendAndReceiveValue( LocalDateTime.now(), Value::asLocalDateTime );
    }

    @Test
    public void shouldSendAndReceiveRandomLocalDateTime()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomLocalDateTime, Value::asLocalDateTime );
    }

    @Test
    public void shouldSendDateTimeWithZoneOffset()
    {
        ZoneOffset offset = ZoneOffset.ofHoursMinutes( -4, -15 );
        testSendValue( ZonedDateTime.of( 1845, 3, 25, 19, 15, 45, 22, offset ), Value::asZonedDateTime );
    }

    @Test
    public void shouldReceiveDateTimeWithZoneOffset()
    {
        ZoneOffset offset = ZoneOffset.ofHoursMinutes( 3, 30 );
        testReceiveValue( "RETURN datetime({year:1984, month:10, day:11, hour:21, minute:30, second:34, timezone:'+03:30'})",
                ZonedDateTime.of( 1984, 10, 11, 21, 30, 34, 0, offset ),
                Value::asZonedDateTime );
    }

    @Test
    public void shouldSendAndReceiveDateTimeWithZoneOffset()
    {
        ZoneOffset offset = ZoneOffset.ofHoursMinutes( -7, -15 );
        testSendAndReceiveValue( ZonedDateTime.of( 2017, 3, 9, 11, 12, 13, 14, offset ), Value::asZonedDateTime );
    }

    @Test
    public void shouldSendAndReceiveRandomDateTimeWithZoneOffset()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomZonedDateTimeWithOffset, Value::asZonedDateTime );
    }

    @Test
    public void shouldSendDateTimeWithZoneId()
    {
        ZoneId zoneId = ZoneId.of( "Europe/Stockholm" );
        testSendValue( ZonedDateTime.of( 2049, 9, 11, 19, 10, 40, 20, zoneId ), Value::asZonedDateTime );
    }

    @Test
    public void shouldReceiveDateTimeWithZoneId()
    {
        ZoneId zoneId = ZoneId.of( "Europe/London" );
        testReceiveValue( "RETURN datetime({year:2000, month:1, day:1, hour:9, minute:5, second:1, timezone:'Europe/London'})",
                ZonedDateTime.of( 2000, 1, 1, 9, 5, 1, 0, zoneId ),
                Value::asZonedDateTime );
    }

    @Test
    public void shouldSendAndReceiveDateTimeWithZoneId()
    {
        ZoneId zoneId = ZoneId.of( "Europe/Stockholm" );
        testSendAndReceiveValue( ZonedDateTime.of( 2099, 12, 29, 12, 59, 59, 59, zoneId ), Value::asZonedDateTime );
    }

    @Test
    public void shouldSendAndReceiveRandomDateTimeWithZoneId()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomZonedDateTimeWithZoneId, Value::asZonedDateTime );
    }

    @Test
    public void shouldSendDuration()
    {
        testSendValue( newDuration( 8, 12, 90, 8 ), Value::asIsoDuration );
    }

    @Test
    public void shouldReceiveDuration()
    {
        testReceiveValue( "RETURN duration({months: 13, days: 40, seconds: 12, nanoseconds: 999})",
                newDuration( 13, 40, 12, 999 ),
                Value::asIsoDuration );
    }

    @Test
    public void shouldSendAndReceiveDuration()
    {
        testSendAndReceiveValue( newDuration( 7, 7, 88, 999_999 ), Value::asIsoDuration );
    }

    @Test
    public void shouldSendAndReceiveRandomDuration()
    {
        testSendAndReceiveRandomValues( TemporalUtil::randomDuration, Value::asIsoDuration );
    }

    private <T> void testSendAndReceiveRandomValues( Supplier<T> supplier, Function<Value,T> converter )
    {
        for ( int i = 0; i < RANDOM_VALUES_TO_TEST; i++ )
        {
            testSendAndReceiveValue( supplier.get(), converter );
        }
    }

    private <T> void testSendValue( T value, Function<Value,T> converter )
    {
        Record record1 = session.run( "CREATE (n:Node {value: $value}) RETURN 42", singletonMap( "value", value ) ).single();
        assertEquals( 42, record1.get( 0 ).asInt() );

        Record record2 = session.run( "MATCH (n:Node) RETURN n.value" ).single();
        assertEquals( value, converter.apply( record2.get( 0 ) ) );
    }

    private <T> void testReceiveValue( String query, T expectedValue, Function<Value,T> converter )
    {
        Record record = session.run( query ).single();
        assertEquals( expectedValue, converter.apply( record.get( 0 ) ) );
    }

    private <T> void testSendAndReceiveValue( T value, Function<Value,T> converter )
    {
        Record record = session.run( "CREATE (n:Node {value: $value}) RETURN n.value", singletonMap( "value", value ) ).single();
        assertEquals( value, converter.apply( record.get( 0 ) ) );
    }

    private static IsoDuration newDuration( long months, long days, long seconds, int nanoseconds )
    {
        return isoDuration( months, days, seconds, nanoseconds ).asIsoDuration();
    }
}
