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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;
import org.neo4j.driver.util.TestUtil;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.ofInteger;
import static org.neo4j.driver.Values.ofValue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;

@ParallelizableIT
class ParametersIT
{
    private static final int LONG_VALUE_SIZE = 1_000_000;

    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldBeAbleToSetAndReturnBooleanProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", true ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().BOOLEAN() ), equalTo( true ) );
            assertThat( value.asBoolean(), equalTo( true ) );
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnByteProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", (byte) 1 ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnShortProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", (short) 1 ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnIntegerProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", 1 ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }

    }

    @Test
    void shouldBeAbleToSetAndReturnLongProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", 1L ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }

    }

    @Test
    void shouldBeAbleToSetAndReturnDoubleProperty()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", 6.28 ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().FLOAT() ), equalTo( true ) );
            assertThat( value.asDouble(), equalTo( 6.28 ) );
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnBytesProperty()
    {
        testBytesProperty( new byte[0] );
        for ( int i = 0; i < 16; i++ )
        {
            int length = (int) Math.pow( 2, i );
            testBytesProperty( randomByteArray( length ) );
            testBytesProperty( randomByteArray( length - 1 ) );
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnStringProperty()
    {
        testStringProperty( "" );
        testStringProperty( "π≈3.14" );
        testStringProperty( "Mjölnir" );
        testStringProperty( "*** Hello World! ***" );
    }

    @Test
    void shouldBeAbleToSetAndReturnBooleanArrayProperty()
    {
        // When
        boolean[] arrayValue = new boolean[]{true, true, true};
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().LIST() ), equalTo( true ) );
            assertThat( value.size(), equalTo( 3 ) );
            for ( Value item : value.asList( ofValue() ) )
            {
                assertThat( item.hasType( session.typeSystem().BOOLEAN() ), equalTo( true ) );
                assertThat( item.asBoolean(), equalTo( true ) );
            }
        }

    }

    @Test
    void shouldBeAbleToSetAndReturnIntegerArrayProperty()
    {
        // When
        int[] arrayValue = new int[]{42, 42, 42};
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().LIST() ), equalTo( true ) );
            assertThat( value.size(), equalTo( 3 ) );
            for ( Value item : value.asList( ofValue() ) )
            {
                assertThat( item.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
                assertThat( item.asLong(), equalTo( 42L ) );
            }
        }

    }

    @Test
    void shouldBeAbleToSetAndReturnDoubleArrayProperty()
    {
        // When
        double[] arrayValue = new double[]{6.28, 6.28, 6.28};
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().LIST() ), equalTo( true ) );
            assertThat( value.size(), equalTo( 3 ) );
            for ( Value item : value.asList( ofValue()) )
            {
                assertThat( item.hasType( session.typeSystem().FLOAT() ), equalTo( true ) );
                assertThat( item.asDouble(), equalTo( 6.28 ) );
            }
        }
    }

    @Test
    void shouldBeAbleToSetAndReturnStringArrayProperty()
    {
        testStringArrayContaining( "cat" );
        testStringArrayContaining( "Mjölnir" );
    }

    private static void testStringArrayContaining( String str )
    {
        String[] arrayValue = new String[]{str, str, str};

        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", arrayValue ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().LIST() ), equalTo( true ) );
            assertThat( value.size(), equalTo( 3 ) );
            for ( Value item : value.asList( ofValue()) )
            {
                assertThat( item.hasType( session.typeSystem().STRING() ), equalTo( true ) );
                assertThat( item.asString(), equalTo( str ) );
            }
        }
    }

    @Test
    void shouldHandleLargeString()
    {
        // Given
        char[] bigStr = new char[1024 * 10];
        for ( int i = 0; i < bigStr.length; i+=4 )
        {
            bigStr[i] = 'a';
            bigStr[i+1] = 'b';
            bigStr[i+2] = 'c';
            bigStr[i+3] = 'd';
        }

        String bigString = new String( bigStr );

        // When
        Value val = session.run( "RETURN $p AS p", parameters( "p", bigString ) ).peek().get( "p" );

        // Then
        assertThat( val.asString(), equalTo( bigString ) );
    }

    @Test
    void shouldBeAbleToSetAndReturnBooleanPropertyWithinMap()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value.v}) RETURN a.value",
                parameters( "value", parameters( "v", true ) ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().BOOLEAN() ), equalTo( true ) );
            assertThat( value.asBoolean(), equalTo( true ) );
        }

    }

    @Test
    void shouldBeAbleToSetAndReturnIntegerPropertyWithinMap()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value.v}) RETURN a.value",
                parameters( "value", parameters( "v", 42 ) ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 42L ) );
        }

    }

    @Test
    void shouldBeAbleToSetAndReturnDoublePropertyWithinMap()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value.v}) RETURN a.value",
                parameters( "value", parameters( "v", 6.28 ) ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().FLOAT() ), equalTo( true ) );
            assertThat( value.asDouble(), equalTo( 6.28 ) );
        }

    }

    @Test
    void shouldBeAbleToSetAndReturnStringPropertyWithinMap()
    {
        // When
        Result result = session.run(
                "CREATE (a {value:$value.v}) RETURN a.value",
                parameters( "value", parameters( "v", "Mjölnir" ) ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().STRING() ), equalTo( true ) );
            assertThat( value.asString(), equalTo( "Mjölnir" ) );
        }
    }

    @Test
    void settingInvalidParameterTypeShouldThrowHelpfulError()
    {
        ClientException e = assertThrows( ClientException.class, () -> session.run( "anything", parameters( "k", new Object() ) ) );
        assertEquals( "Unable to convert java.lang.Object to Neo4j Value.", e.getMessage() );
    }

    @Test
    void settingInvalidParameterTypeDirectlyShouldThrowHelpfulError()
    {
        IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> session.run( "anything", emptyNodeValue() ) );
        assertEquals( "The parameters should be provided as Map type. Unsupported parameters type: NODE", e.getMessage() );
    }

    @Test
    void shouldNotBePossibleToUseNodeAsParameterInMapValue()
    {
        // GIVEN
        Value node = emptyNodeValue();
        Map<String,Value> params = new HashMap<>();
        params.put( "a", node );
        MapValue mapValue = new MapValue( params );

        // WHEN
        expectIOExceptionWithMessage( mapValue, "Unknown type: NODE" );
    }

    @Test
    void shouldNotBePossibleToUseRelationshipAsParameterViaMapValue()
    {
        // GIVEN
        Value relationship = emptyRelationshipValue();
        Map<String,Value> params = new HashMap<>();
        params.put( "a", relationship );
        MapValue mapValue = new MapValue( params );

        // WHEN
        expectIOExceptionWithMessage( mapValue, "Unknown type: RELATIONSHIP" );
    }

    @Test
    void shouldNotBePossibleToUsePathAsParameterViaMapValue()
    {
        // GIVEN
        Value path = filledPathValue();
        Map<String,Value> params = new HashMap<>();
        params.put( "a", path );
        MapValue mapValue = new MapValue( params );

        // WHEN
        expectIOExceptionWithMessage( mapValue, "Unknown type: PATH" );
    }

    @Test
    void shouldSendAndReceiveLongString()
    {
        String string = TestUtil.randomString( LONG_VALUE_SIZE );
        testSendAndReceiveValue( string );
    }

    @Test
    void shouldSendAndReceiveLongListOfLongs()
    {
        List<Long> longs = ThreadLocalRandom.current()
                .longs( LONG_VALUE_SIZE )
                .boxed()
                .collect( toList() );

        testSendAndReceiveValue( longs );
    }

    @Test
    void shouldSendAndReceiveLongArrayOfBytes()
    {
        byte[] bytes = new byte[LONG_VALUE_SIZE];
        ThreadLocalRandom.current().nextBytes( bytes );

        testSendAndReceiveValue( bytes );
    }

    @Test
    void shouldAcceptStreamsAsQueryParameters()
    {
        Stream<Integer> stream = Stream.of( 1, 2, 3, 4, 5, 42 );

        Result result = session.run( "RETURN $value", singletonMap( "value", stream ) );
        Value receivedValue = result.single().get( 0 );

        assertEquals( asList( 1, 2, 3, 4, 5, 42 ), receivedValue.asList( ofInteger() ) );
    }

    private static void testBytesProperty( byte[] array )
    {
        Result result = session.run( "CREATE (a {value:$value}) RETURN a.value", parameters( "value", array ) );

        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().BYTES() ), equalTo( true ) );
            assertThat( value.asByteArray(), equalTo( array ) );
        }
    }

    private static void testStringProperty( String string )
    {
        Result result = session.run(
                "CREATE (a {value:$value}) RETURN a.value", parameters( "value", string ) );

        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().STRING() ), equalTo( true ) );
            assertThat( value.asString(), equalTo( string ) );
        }
    }

    private static byte[] randomByteArray( int length )
    {
        byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes( result );
        return result;
    }

    private static void expectIOExceptionWithMessage( Value value, String message )
    {
        ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> session.run( "RETURN {a}", value ).consume() );
        Throwable cause = e.getCause();
        assertThat( cause, instanceOf( IOException.class ) );
        assertThat( cause.getMessage(), equalTo( message ) );
    }

    private static void testSendAndReceiveValue( Object value )
    {
        Result result = session.run( "RETURN $value", singletonMap( "value", value ) );
        Object receivedValue = result.single().get( 0 ).asObject();
        assertArrayEquals( new Object[]{value}, new Object[]{receivedValue} );
    }
}
