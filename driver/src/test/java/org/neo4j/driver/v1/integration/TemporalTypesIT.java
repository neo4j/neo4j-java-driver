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
package org.neo4j.driver.v1.integration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.function.Function;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Duration;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static java.time.Month.MARCH;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.internal.util.ServerVersion.v3_4_0;

public class TemporalTypesIT
{
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
    public void shouldSendDuration()
    {
        testSendValue( newDuration( 8, 12, 90, 8 ), Value::asDuration );
    }

    @Test
    public void shouldReceiveDuration()
    {
        testReceiveValue( "RETURN duration({months: 13, days: 40, seconds: 12, nanoseconds: 999})",
                newDuration( 13, 40, 12, 999 ),
                Value::asDuration );
    }

    @Test
    public void shouldSendAndReceiveDuration()
    {
        testSendAndReceiveValue( newDuration( 7, 7, 88, 999_999 ), Value::asDuration );
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

    private static Duration newDuration( long months, long days, long seconds, long nanoseconds )
    {
        return Values.duration( months, days, seconds, nanoseconds ).asDuration();
    }
}
