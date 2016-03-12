/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.Values.ofValue;

public class ParametersIT
{
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
    public void shouldBeAbleToSetAndReturnStringProperty()
    {
        // When
        StatementResult result = session.run(
                "CREATE (a {value:{value}}) RETURN a.value", parameters( "value", "Mjölnir" ) );

        // Then
        for ( Record record : result.list() )
        {
            Value value = record.get( "a.value" );
            assertThat( value.hasType( session.typeSystem().STRING() ), equalTo( true ) );
            assertThat( value.asString(), equalTo( "Mjölnir" ) );
        }

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
    public void shouldNotBePossibleToUseNodeAsParameter()
    {
        // GIVEN
        StatementResult cursor = session.run( "CREATE (a:Node) RETURN a" );
        Node node = cursor.single().get( 0 ).asNode();

        //Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Nodes can't be used as parameters" );

        // WHEN
        session.run( "RETURN {a}", parameters( "a", node ) );
    }

    @Test
    public void shouldNotBePossibleToUseRelationshipAsParameter()
    {
        // GIVEN
        StatementResult cursor = session.run( "CREATE (a:Node), (b:Node), (a)-[r:R]->(b) RETURN r" );
        Relationship relationship = cursor.single().get( 0 ).asRelationship();

        //Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Relationships can't be used as parameters" );

        // WHEN
        session.run( "RETURN {a}", parameters( "a", relationship ) );
    }

    @Test
    public void shouldNotBePossibleToUsePathAsParameter()
    {
        // GIVEN
        StatementResult cursor = session.run( "CREATE (a:Node), (b:Node), p=(a)-[r:R]->(b) RETURN p" );
        Path path = cursor.single().get( 0 ).asPath();

        //Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Paths can't be used as parameters" );

        // WHEN
        session.run( "RETURN {a}", parameters( "a", path ) );
    }
}
