/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.util.TestNeo4jSession;
import org.neo4j.driver.v1.util.TestUtil;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.driver.internal.util.ServerVersion.version;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.emptyRelationshipValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledPathValue;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.parameters;

public class ParametersIT
{
    private static final int LONG_VALUE_SIZE = 1_000_000;

    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldBeAbleToSetAndReturnBooleanProperty()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", true ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().BOOLEAN() ), equalTo( true ) );
            assertThat( value.asBoolean(), equalTo( true ) );
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnByteProperty()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", (byte) 1 ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnShortProperty()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", (short) 1 ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnIntegerProperty()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", 1 ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnLongProperty()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", 1L ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().INTEGER() ), equalTo( true ) );
            assertThat( value.asLong(), equalTo( 1L ) );
        }

    }

    @Test
    public void shouldBeAbleToSetAndReturnDoubleProperty()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", 6.28 ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().FLOAT() ), equalTo( true ) );
            assertThat( value.asDouble(), equalTo( 6.28 ) );
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnBytesProperty()
    {
        assumeTrue( supportsBytes() );

        testBytesProperty( new byte[0] );
        for ( int i = 0; i < 16; i++ )
        {
            int length = (int) Math.pow( 2, i );
            testBytesProperty( randomByteArray( length ) );
            testBytesProperty( randomByteArray( length - 1 ) );
        }
    }

    @Test
    public void shouldThrowExceptionWhenServerDoesNotSupportBytes()
    {
        assumeFalse( supportsBytes() );

        // Given
        byte[] byteArray = "hello, world".getBytes();

        // When
        try
        {
            StatementResult result = session.run(
                    "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", byteArray ) );
            result.single();
            fail( "Should not be able to pack bytes" );
        }
        catch ( ServiceUnavailableException e )
        {
            assertThat( e.getCause().getMessage(), containsString( "Packing bytes is not supported" ) );
        }
    }

    @Test
    public void shouldBeAbleToSetAndReturnStringProperty()
    {
        testStringProperty( "" );
        testStringProperty( "π≈3.14" );
        testStringProperty( "Mjölnir" );
        testStringProperty( "*** Hello World! ***" );
    }

    @Test
    public void shouldBeAbleToSetAndReturnBooleanArrayProperty()
    {
        // When
        boolean[] arrayValue = new boolean[]{true, true, true};
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

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
    public void shouldBeAbleToSetAndReturnIntegerArrayProperty()
    {
        // When
        int[] arrayValue = new int[]{42, 42, 42};
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

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
    public void shouldBeAbleToSetAndReturnDoubleArrayProperty()
    {
        // When
        double[] arrayValue = new double[]{6.28, 6.28, 6.28};
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

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
    public void shouldBeAbleToSetAndReturnStringArrayProperty()
    {
        testStringArrayContaining( "cat" );
        testStringArrayContaining( "Mjölnir" );
    }

    private void testStringArrayContaining( String str )
    {
        String[] arrayValue = new String[]{str, str, str};

        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", arrayValue ) );

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
    public void shouldHandleLargeString() throws Throwable
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
        Value val = session.run( "RETURN {p} AS p", parameters( "p", bigString ) ).peek().get( "p" );

        // Then
        assertThat( val.asString(), equalTo( bigString ) );
    }

    @Test
    public void shouldBeAbleToSetAndReturnBooleanPropertyWithinMap()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}.v}) RETURN a.value",
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
    public void shouldBeAbleToSetAndReturnIntegerPropertyWithinMap()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}.v}) RETURN a.value",
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
    public void shouldBeAbleToSetAndReturnDoublePropertyWithinMap()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}.v}) RETURN a.value",
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
    public void shouldBeAbleToSetAndReturnStringPropertyWithinMap()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}.v}) RETURN a.value",
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
    public void settingInvalidParameterTypeShouldThrowHelpfulError() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Unable to convert java.lang.Object to Neo4j Value." );

        // When
        session.run( "anything", parameters( "k", new Object() ) );
    }

    @Test
    public void settingInvalidParameterTypeDirectlyShouldThrowHelpfulError() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "The parameters should be provided as Map type. Unsupported parameters type: NODE" );

        // When
        session.run( "anything", emptyNodeValue() );
    }

    @Test
    public void shouldNotBePossibleToUseNodeAsParameterInMapValue()
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
    public void shouldNotBePossibleToUseRelationshipAsParameterViaMapValue()
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
    public void shouldNotBePossibleToUsePathAsParameterViaMapValue()
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
    public void shouldSendAndReceiveLongString()
    {
        String string = TestUtil.randomString( LONG_VALUE_SIZE );
        testSendAndReceiveValue( string );
    }

    @Test
    public void shouldSendAndReceiveLongListOfLongs()
    {
        List<Long> longs = ThreadLocalRandom.current()
                .longs( LONG_VALUE_SIZE )
                .boxed()
                .collect( toList() );

        testSendAndReceiveValue( longs );
    }

    @Test
    public void shouldSendAndReceiveLongArrayOfBytes()
    {
        assumeTrue( supportsBytes() );

        byte[] bytes = new byte[LONG_VALUE_SIZE];
        ThreadLocalRandom.current().nextBytes( bytes );

        testSendAndReceiveValue( bytes );
    }

    private void testBytesProperty( byte[] array )
    {
        assumeTrue( supportsBytes() );

        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", array ) );

        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().BYTES() ), equalTo( true ) );
            assertThat( value.asByteArray(), equalTo( array ) );
        }
    }

    private void testStringProperty( String string )
    {
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", string ) );

        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().STRING() ), equalTo( true ) );
            assertThat( value.asString(), equalTo( string ) );
        }
    }

    private boolean supportsBytes()
    {
        return version( session.driver() ).greaterThanOrEqual( ServerVersion.v3_2_0 );
    }

    private static byte[] randomByteArray( int length )
    {
        byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes( result );
        return result;
    }

    private void expectIOExceptionWithMessage( Value value, String message )
    {
        try
        {
            session.run( "RETURN {a}", value ).consume();
            fail( "Expecting a ServiceUnavailableException" );
        }
        catch ( ServiceUnavailableException e )
        {
            Throwable cause = e.getCause();
            assertThat( cause, instanceOf( IOException.class ) );
            assertThat( cause.getMessage(), equalTo( message ) );
        }
        catch ( Exception e )
        {
            fail( "Expecting a ServiceUnavailableException but got " + e );
        }
    }

    private void testSendAndReceiveValue( Object value )
    {
        StatementResult result = session.run( "RETURN $value", singletonMap( "value", value ) );
        Object receivedValue = result.single().get( 0 ).asObject();
        assertArrayEquals( new Object[]{value}, new Object[]{receivedValue} );
    }
}
