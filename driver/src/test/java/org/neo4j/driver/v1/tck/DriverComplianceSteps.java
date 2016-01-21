/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * <p>
 * This file is part of Neo4j.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.v1.tck;

import cucumber.api.DataTable;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import cucumber.runtime.CucumberException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Relationship;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.tck.tck.util.runners.CypherStatementRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.MappedParametersRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.StatementRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.StringRunner;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Collections.singletonMap;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.tck.DriverComplianceIT.session;
import static org.neo4j.driver.v1.tck.Environment.expectedBoltValue;
import static org.neo4j.driver.v1.tck.Environment.expectedJavaValue;
import static org.neo4j.driver.v1.tck.Environment.listOfObjects;
import static org.neo4j.driver.v1.tck.Environment.mapOfObjects;
import static org.neo4j.driver.v1.tck.Environment.mappedParametersRunner;
import static org.neo4j.driver.v1.tck.Environment.runners;
import static org.neo4j.driver.v1.tck.Environment.statementRunner;
import static org.neo4j.driver.v1.tck.Environment.stringRunner;


public class DriverComplianceSteps
{
    @Given( "^A running database$" )
    public void A_running_database() throws Throwable
    {
        if ( !databaseRunning() )
        {
            throw new CucumberException( "Database is not running" );
        }
    }

    @Given( "^a value ([^\"]*) of type ([^\"]*)$" )
    public void a_value_of_Type( String value, String type )
            throws Throwable
    {
        expectedJavaValue = asJavaType( type, value );
        expectedBoltValue = getBoltValue( type, value );
    }

    @Given( "^a String of size (\\d+)$" )
    public void a_String_of_size( long size ) throws Throwable
    {
        expectedJavaValue = getRandomString( size );
        expectedBoltValue = Values.value( expectedJavaValue );
    }

    @Given( "^a List of size (\\d+) and type ([^\"]*)$" )
    public void a_List_of_size_and_type_Type( long size, String type ) throws Throwable
    {
        expectedJavaValue = getListOfRandomsOfTypes( type, size );
        expectedBoltValue = Values.value( expectedJavaValue );
    }

    @Given( "^a Map of size (\\d+) and type ([^\"]*)$" )
    public void a_Map_of_size_and_type_Type( long size, String type ) throws Throwable
    {
        expectedJavaValue = getMapOfRandomsOfTypes( type, size );
        expectedBoltValue = Values.value( expectedJavaValue );
    }

    @Given( "^a list value ([^\"]*) of type ([^\"]*)$" )
    public void a_list_value_of_Type( String value, String type )
            throws Throwable
    {
        expectedJavaValue = asJavaArrayList( type, getListFromString( value ) );
        expectedBoltValue = Values.value( expectedJavaValue );
    }

    @And( "^the expected result is a bolt \"([^\"]*)\" of \"([^\"]*)\"$" )
    public void the_expected_result_is_a_of( String type, String value ) throws Throwable
    {
        expectedJavaValue = asJavaType( type, value );
        expectedBoltValue = getBoltValue( type, value );
    }

    @When( "^the driver asks the server to echo this value back$" )
    public void the_driver_asks_the_server_to_echo_this_value_back() throws Throwable
    {
        stringRunner = new StringRunner( "RETURN " + boltValueAsCypherString( expectedBoltValue ) );
        mappedParametersRunner = new MappedParametersRunner( "RETURN {input}", "input", expectedBoltValue );
        statementRunner = new StatementRunner(
                new Statement( "RETURN {input}", singletonMap( "input", expectedBoltValue ) ) );

        runners.add( stringRunner );
        runners.add( mappedParametersRunner );
        runners.add( statementRunner );

        for ( CypherStatementRunner runner : runners )
        {
            runner.runCypherStatement();
        }
    }

    @When( "^the driver asks the server to echo this list back$" )
    public void the_driver_asks_the_server_to_echo_this_list_back() throws Throwable
    {
        expectedJavaValue = listOfObjects;
        expectedBoltValue = Values.value( expectedJavaValue );
        the_driver_asks_the_server_to_echo_this_value_back();
    }

    @When( "^the driver asks the server to echo this map back$" )
    public void the_driver_asks_the_server_to_echo_this_map_back() throws Throwable
    {
        expectedJavaValue = mapOfObjects;
        expectedBoltValue = Values.value( expectedJavaValue );
        the_driver_asks_the_server_to_echo_this_value_back();
    }

    @Then( "^the value given in the result should be the same as what was sent" )
    public void result_should_be_equal_to_a_single_Type_of_Input() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            assertTrue( runner.result().single() );
            Value resultBoltValue = runner.result().record().value( 0 );
            Object resultJavaValue = boltValuetoJavaObject( expectedBoltValue );

            assertThat( resultBoltValue, equalTo( expectedBoltValue ) );

            assertThat( resultJavaValue, equalTo( expectedJavaValue ) );
        }
    }

    @When( "^adding a table of lists to the list L$" )
    public void adding_a_table_of_lists_to_the_list_of_objects( DataTable table ) throws Throwable
    {
        Map<String,String> map = table.asMap( String.class, String.class );
        for ( String type : map.keySet() )
        {
            listOfObjects.add( asJavaArrayList( type, getListFromString( map.get( type ) ) ) );
        }
    }

    @When( "^adding a table of values to the list L$" )
    public void adding_a_table_of_values_to_the_list_of_objects( DataTable table ) throws Throwable
    {
        Map<String,String> map = table.asMap( String.class, String.class );
        for ( String type : map.keySet() )
        {
            listOfObjects.add( asJavaType( type, map.get( type ) ) );
        }
    }

    @When( "^adding a table of lists to the map M$" )
    public void adding_a_table_of_lists_to_the_map_of_objects( DataTable table ) throws Throwable
    {
        Map<String,String> map = table.asMap( String.class, String.class );
        for ( String type : map.keySet() )
        {
            mapOfObjects.put( "a" + valueOf( mapOfObjects.size() ), (asJavaArrayList( type,
                    getListFromString(
                            map.get( type ) ) )) );
        }
    }

    @When( "^adding a table of values to the map M$" )
    public void adding_a_table_of_values_to_the_map_of_objects( DataTable table ) throws Throwable
    {
        Map<String,String> map = table.asMap( String.class, String.class );
        for ( String type : map.keySet() )
        {
            mapOfObjects.put( "a" + valueOf( mapOfObjects.size() ), (asJavaType( type, map.get( type ) )) );
        }
    }

    @When( "^adding a copy of map M to map M$" )
    public void adding_map_of_objects_to_map_of_objects() throws Throwable
    {
        mapOfObjects.put( "a" + valueOf( mapOfObjects.size() ), new HashMap<>( mapOfObjects ) );
    }

    @When( "^adding map M to list L$" )
    public void adding_map_of_objects_to_list_of_objects() throws Throwable
    {
        listOfObjects.add( mapOfObjects );
    }

    @And( "^an empty map M$" )
    public void a_map_of_objects() throws Throwable
    {
        mapOfObjects = new HashMap<>();
    }

    @And( "^an empty list L$" )
    public void a_list_of_objects() throws Throwable
    {
        listOfObjects = new ArrayList<>();
    }


    @And( "^the relationship value given in the result should be the same as what was sent$" )
    public void the_relationship_value_given_in_the_result_should_be_the_same_as_what_was_sent() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            assertTrue( runner.result().single() );
            Value receivedValue = runner.result().record().value( 0 );
            Relationship relationship = receivedValue.asRelationship();
            Relationship expectedRelationship = expectedBoltValue.asRelationship();

            assertThat( relationship.identity(), equalTo( expectedRelationship.identity() ) );
            assertThat( relationship.start(), equalTo( expectedRelationship.start() ) );
            assertThat( relationship.properties(), equalTo( expectedRelationship.properties() ) );
            assertThat( relationship.end(), equalTo( expectedRelationship.end() ) );
            assertThat( relationship.properties(), equalTo( expectedRelationship.properties() ) );
            assertThat( receivedValue, equalTo( expectedBoltValue ) );
        }
    }


    @And( "^the path value given in the result should be the same as what was sent$" )
    public void the_path_value_given_in_the_result_should_be_the_same_as_what_was_sent() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            assertTrue( runner.result().single() );
            Value receivedValue = runner.result().record().value( 0 );
            Path path = receivedValue.asPath();
            Path expectedPath = expectedBoltValue.asPath();

            assertThat( path.start(), equalTo( expectedPath.start() ) );
            assertThat( path.end(), equalTo( expectedPath.end() ) );
            assertThat( path.nodes(), equalTo( expectedPath.nodes() ) );
            assertThat( path.relationships(), equalTo( expectedPath.relationships() ) );
            assertThat( receivedValue, equalTo( expectedBoltValue ) );
        }
    }


    public Value getBoltValue( String type, String var )
    {
        Object o = asJavaType( type, var );
        if ( o == null )
        {
            return NullValue.NULL;
        }

        else
        {
            return Values.value( o );
        }
    }

    public ArrayList<Object> asJavaArrayList( String type, String[] array )
    {
        ArrayList<Object> values = new ArrayList<>();
        for ( String str : array )
        {
            values.add( asJavaType( type, str ) );
        }
        return values;
    }

    public Object asJavaType( String type, String var )
    {
        switch ( type )
        {
        case "Integer":
            return Long.valueOf( var );
        case "String":
            return var;
        case "Boolean":
            return Boolean.valueOf( var );
        case "Float":
            return Double.valueOf( var );
        case "Null":
        {
            return null;
        }
        default:
            throw new NoSuchElementException( format( "Unrecognised type: [%s]", type ) );
        }
    }

    public Object boltValuetoJavaObject( Value val )
    {
        if ( val.getClass().equals( IntegerValue.class ) )
        {
            return Values.valueAsLong().apply( val );
        }
        else if ( val.getClass().equals( StringValue.class ) )
        {
            return Values.valueAsString().apply( val );
        }
        else if ( val.getClass().equals( BooleanValue.class ) || val.getClass().equals( BooleanValue.TRUE.getClass() )
                  || val.getClass().equals( BooleanValue.FALSE.getClass() ) )
        {
            return Values.valueAsBoolean().apply( val );
        }
        else if ( val.getClass().equals( FloatValue.class ) )
        {
            return Values.valueAsDouble().apply( val );
        }
        else if ( val.getClass().equals( NullValue.class ) )
        {
            return null;
        }
        else if ( val.getClass().equals( ListValue.class ) )
        {
            List<Object> object = new ArrayList<>();
            for ( Value value : Values.valueAsList( Values.valueAsIs() ).apply( val ) )
            {
                object.add( boltValuetoJavaObject( value ) );
            }
            return object;
        }
        else if ( val.getClass().equals( MapValue.class ) )
        {
            Map<String,Value> value_map = val.asMap( Values.valueAsIs() );
            Map<String,Object> object = new HashMap<>();
            for ( String key : value_map.keySet() )
            {
                object.put( key, boltValuetoJavaObject( value_map.get( key ) ) );
            }
            return object;
        }
        else
        {
            throw new NoSuchElementException( format( "Unrecognised bolt value: %s with class:%s", val, val
                    .getClass() ) );
        }
    }

    public String getRandomString( long size )
    {
        StringBuilder stringBuilder = new StringBuilder();
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        Random random = new Random();
        while ( size-- > 0 )
        {
            stringBuilder.append( alphabet.charAt( random.nextInt( alphabet.length() ) ) );
        }
        return stringBuilder.toString();
    }

    public List<Object> getListOfRandomsOfTypes( String type, long size )
    {
        List<Object> list = new ArrayList<>();
        while ( size-- > 0 )
        {
            list.add( getRandomValue( type ) );
        }
        return list;
    }

    public Map<String,Object> getMapOfRandomsOfTypes( String type, long size )
    {
        Map<String,Object> map = new HashMap<>();
        while ( size-- > 0 )
        {
            map.put( "a" + size, getRandomValue( type ) );
        }
        return map;
    }

    public Object getRandomValue( String type )
    {
        switch ( type )
        {
        case "Integer":
            return getRandomLong();
        case "String":
            return getRandomString( 3 );
        case "Boolean":
            return getRandomBoolean();
        case "Float":
            return getRandomDouble();
        case "Null":
            return null;
        default:
            throw new NoSuchElementException( format( "Not supported type: %s", type ) );
        }
    }

    public long getRandomLong()
    {
        Random random = new Random();
        return random.nextLong();
    }

    public double getRandomDouble()
    {
        Random random = new Random();
        return random.nextDouble();
    }

    public boolean getRandomBoolean()
    {
        Random random = new Random();
        return random.nextBoolean();
    }

    public String boltValueAsCypherString( Value value )
    {
        return value.asLiteralString();
    }

    public String[] getListFromString( String str )
    {
        return str.replaceAll( "\\[", "" )
                .replaceAll( "\\]", "" )
                .split( "," );
    }

    public boolean databaseRunning()
    {
        return session() != null;
    }
}
