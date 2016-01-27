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
import java.util.Random;

import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.tck.tck.util.runners.CypherStatementRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.MappedParametersRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.StatementRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.StringRunner;

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
import static org.neo4j.driver.v1.tck.tck.util.Types.Type;
import static org.neo4j.driver.v1.tck.tck.util.Types.getType;


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
        expectedJavaValue = getType( type ).getJavaValue( value );
        expectedBoltValue = Values.value( expectedJavaValue );
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
        expectedJavaValue = getListOfRandomsOfTypes( getType( type ), size );
        expectedBoltValue = Values.value( expectedJavaValue );
    }

    @Given( "^a Map of size (\\d+) and type ([^\"]*)$" )
    public void a_Map_of_size_and_type_Type( long size, String type ) throws Throwable
    {
        expectedJavaValue = getMapOfRandomsOfTypes( getType( type ), size );
        expectedBoltValue = Values.value( expectedJavaValue );
    }

    @Given( "^a list value ([^\"]*) of type ([^\"]*)$" )
    public void a_list_value_of_Type( String value, String type ) throws Throwable
    {
        expectedJavaValue = getType( type ).getJavaArrayList( getListFromString( value ) );
        expectedBoltValue = Values.value( expectedJavaValue );
    }

    @And( "^the expected result is a bolt \"([^\"]*)\" of \"([^\"]*)\"$" )
    public void the_expected_result_is_a_of( String type, String value ) throws Throwable
    {
        expectedJavaValue = getType( type ).getJavaValue( value );
        expectedBoltValue = Values.value( expectedJavaValue );
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

    @When( "^adding a table of lists to the list L$" )
    public void adding_a_table_of_lists_to_the_list_of_objects( DataTable table ) throws Throwable
    {
        Map<String,String> map = table.asMap( String.class, String.class );
        for ( String type : map.keySet() )
        {
            listOfObjects.add( getType( type ).getJavaArrayList( getListFromString( map.get( type ) ) ) );
        }
    }

    @When( "^adding a table of values to the list L$" )
    public void adding_a_table_of_values_to_the_list_of_objects( DataTable table ) throws Throwable
    {
        Map<String,String> map = table.asMap( String.class, String.class );
        for ( String type : map.keySet() )
        {
            listOfObjects.add( getType( type ).getJavaValue( map.get( type ) ) );
        }
    }

    @When( "^adding a table of lists to the map M$" )
    public void adding_a_table_of_lists_to_the_map_of_objects( DataTable table ) throws Throwable
    {
        Map<String,String> map = table.asMap( String.class, String.class );
        for ( String type : map.keySet() )
        {
            mapOfObjects.put( "a" + valueOf( mapOfObjects.size() ), getType( type ).getJavaArrayList(
                    getListFromString( map.get( type ) ) ) );
        }
    }

    @When( "^adding a table of values to the map M$" )
    public void adding_a_table_of_values_to_the_map_of_objects( DataTable table ) throws Throwable
    {
        Map<String,String> map = table.asMap( String.class, String.class );
        for ( String type : map.keySet() )
        {
            mapOfObjects.put( "a" + valueOf( mapOfObjects.size() ), getType( type ).getJavaValue( map.get( type ) ) );
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

    @Then( "^the value given in the result should be the same as what was sent" )
    public void result_should_be_equal_to_a_single_Type_of_Input() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            assertTrue( runner.result().single() );
            Value resultBoltValue = runner.result().record().get( 0 );
            Object resultJavaValue = expectedBoltValue.asObject();

            assertThat( resultBoltValue, equalTo( expectedBoltValue ) );

            assertThat( resultJavaValue, equalTo( expectedJavaValue ) );
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

    public List<Object> getListOfRandomsOfTypes( Type type, long size )
    {
        List<Object> list = new ArrayList<>();
        while ( size-- > 0 )
        {
            list.add( type.getRandomValue(  ) );
        }
        return list;
    }

    public Map<String,Object> getMapOfRandomsOfTypes( Type type, long size )
    {
        Map<String,Object> map = new HashMap<>();
        while ( size-- > 0 )
        {
            map.put( "a" + size, type.getRandomValue() );
        }
        return map;
    }

    public String boltValueAsCypherString( Value value )
    {
        return value.toString();
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
