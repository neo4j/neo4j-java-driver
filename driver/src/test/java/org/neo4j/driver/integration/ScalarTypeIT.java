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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.util.ParallelizableIT;
import org.neo4j.driver.util.SessionExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;

@ParallelizableIT
class ScalarTypeIT
{
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    static Stream<Arguments> typesToTest()
    {
        return Stream.of(
                Arguments.of( "RETURN 1 as v", Values.value( 1L ) ),
                Arguments.of( "RETURN 1.1 as v", Values.value( 1.1d ) ),
                Arguments.of( "RETURN 'hello' as v", Values.value( "hello" ) ),
                Arguments.of( "RETURN true as v", Values.value( true ) ),
                Arguments.of( "RETURN false as v", Values.value( false ) ),
                Arguments.of( "RETURN [1,2,3] as v", new ListValue( Values.value( 1 ), Values.value( 2 ), Values.value( 3 ) ) ),
                Arguments.of( "RETURN ['hello'] as v", new ListValue( Values.value( "hello" ) ) ),
                Arguments.of( "RETURN [] as v", new ListValue() ),
                Arguments.of( "RETURN {k:'hello'} as v", parameters( "k", Values.value( "hello" ) ) ),
                Arguments.of( "RETURN {} as v", new MapValue( Collections.<String,Value>emptyMap() ) )
        );
    }

    @ParameterizedTest
    @MethodSource( "typesToTest" )
    void shouldHandleType( String query, Value expectedValue )
    {
        // When
        Result cursor = session.run( query );

        // Then
        assertThat( cursor.single().get( "v" ), equalTo( expectedValue ) );
    }

    static Stream<Arguments> collectionItems()
    {
        return Stream.of(
                Arguments.of( Values.value( (Object) null ) ),
                Arguments.of( Values.value( 1L ) ),
                Arguments.of( Values.value( 1.1d ) ),
                Arguments.of( Values.value( "hello" ) ),
                Arguments.of( Values.value( true ) )
        );
    }

    @ParameterizedTest
    @MethodSource( "collectionItems" )
    void shouldEchoVeryLongMap( Value collectionItem )
    {
        // Given
        Map<String, Value> input = new HashMap<>();
        for ( int i = 0; i < 1000; i ++ )
        {
            input.put( String.valueOf( i ), collectionItem );
        }
        MapValue mapValue = new MapValue( input );

        // When & Then
        verifyCanEncodeAndDecode( mapValue );
    }

    @ParameterizedTest
    @MethodSource( "collectionItems" )
    void shouldEchoVeryLongList( Value collectionItem )
    {
        // Given
        Value[] input = new Value[1000];
        for ( int i = 0; i < 1000; i ++ )
        {
            input[i] = collectionItem;
        }
        ListValue listValue = new ListValue( input );

        // When & Then
        verifyCanEncodeAndDecode( listValue );

    }

    @Test
    void shouldEchoVeryLongString()
    {
        // Given
        char[] chars = new char[10000];
        Arrays.fill( chars, '*' );
        String longText = new String( chars );
        StringValue input = new StringValue( longText );

        // When & Then
        verifyCanEncodeAndDecode( input );
    }

    static Stream<Arguments> scalarTypes()
    {
        return Stream.of(
                Arguments.of( Values.value( (Object) null ) ),
                Arguments.of( Values.value( true ) ),
                Arguments.of( Values.value( false ) ),
                Arguments.of( Values.value( 1L ) ),
                Arguments.of( Values.value( -17L ) ),
                Arguments.of( Values.value( -129L ) ),
                Arguments.of( Values.value( 129L ) ),
                Arguments.of( Values.value( 2147483647L ) ),
                Arguments.of( Values.value( -2147483648L ) ),
                Arguments.of( Values.value( -2147483648L ) ),
                Arguments.of( Values.value( 9223372036854775807L ) ),
                Arguments.of( Values.value( -9223372036854775808L ) ),
                Arguments.of( Values.value( 1.7976931348623157E+308d ) ),
                Arguments.of( Values.value( 2.2250738585072014e-308d ) ),
                Arguments.of( Values.value( 0.0d ) ),
                Arguments.of( Values.value( 1.1d ) ),
                Arguments.of( Values.value( "1" ) ),
                Arguments.of( Values.value( "-17∂ßå®" ) ),
                Arguments.of( Values.value( "String" ) ),
                Arguments.of( Values.value( "" ) )
            );
        }

    @ParameterizedTest
    @MethodSource( "scalarTypes" )
    void shouldEchoScalarTypes( Value input )
    {
        // When & Then
        verifyCanEncodeAndDecode( input );
    }

    static Stream<Arguments> listToTest()
    {
        return Stream.of(
                Arguments.of( Values.value( 1, 2, 3, 4 ) ),
                Arguments.of( Values.value( true, false ) ),
                Arguments.of( Values.value( 1.1, 2.2, 3.3 ) ),
                Arguments.of( Values.value( "a", "b", "c", "˚C" ) ),
                Arguments.of( Values.value( NullValue.NULL, NullValue.NULL ) ),
                Arguments.of( Values.value( Values.values( NullValue.NULL, true, "-17∂ßå®", 1.7976931348623157E+308d, -9223372036854775808d ) ) ),
                Arguments.of( new ListValue( parameters( "a", 1, "b", true, "c", 1.1, "d", "˚C", "e", null ) ) )
        );
    }

    @ParameterizedTest
    @MethodSource( "listToTest" )
    void shouldEchoList( Value input )
    {
        // When & Then
        assertTrue( input instanceof ListValue );
        verifyCanEncodeAndDecode( input );
    }

    @Test
    void shouldEchoNestedList() throws Throwable
    {
        Value input = Values.value( toValueStream( listToTest() ) );

        // When & Then
        assertTrue( input instanceof ListValue );
        verifyCanEncodeAndDecode( input );
    }

    static Stream<Arguments> mapToTest()
    {
        return Stream.of( Arguments.of( parameters( "a", 1, "b", 2, "c", 3, "d", 4 ) ),
                Arguments.of( parameters( "a", true, "b", false ) ),
                Arguments.of( parameters( "a", 1.1, "b", 2.2, "c", 3.3 ) ),
                Arguments.of( parameters( "b", "a", "c", "b", "d", "c", "e", "˚C" ) ),
                Arguments.of( parameters( "a", null ) ),
                Arguments.of( parameters( "a", 1, "b", true, "c", 1.1, "d", "˚C", "e", null ) ) );
    }

    @ParameterizedTest
    @MethodSource( "mapToTest" )
    void shouldEchoMap( Value input )
    {
        assertTrue( input instanceof MapValue );
        // When & Then
        verifyCanEncodeAndDecode( input );
    }

    @Test
    void shouldEchoNestedMap() throws Throwable
    {
        MapValue input = new MapValue(
                toValueStream( mapToTest() )
                .collect( Collectors.toMap( Object::toString, Function.identity() ) ) );

        // When & Then
        verifyCanEncodeAndDecode( input );
    }

    private Stream<Value> toValueStream( Stream<Arguments> arguments )
    {
        return arguments.map( arg -> {
            Object obj = arg.get()[0];
            assertTrue( obj instanceof Value );
            return (Value) obj;
        } );
    }

    private void verifyCanEncodeAndDecode( Value input )
    {
        // When
        Result cursor = session.run( "RETURN $x as y", parameters( "x", input ) );

        // Then
        assertThat( cursor.single().get( "y" ), equalTo( input ) );
    }
}
