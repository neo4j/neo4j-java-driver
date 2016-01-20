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
import cucumber.api.Scenario;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import cucumber.runtime.CucumberException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Relationship;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;

import static java.lang.String.valueOf;
import static java.util.Collections.singletonMap;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.tck.TCKTestUtil.CypherStatementRunner;
import static org.neo4j.driver.v1.tck.TCKTestUtil.MappedParametersRunner;
import static org.neo4j.driver.v1.tck.TCKTestUtil.StatementRunner;
import static org.neo4j.driver.v1.tck.TCKTestUtil.StringRunner;
import static org.neo4j.driver.v1.tck.TCKTestUtil.asJavaArrayList;
import static org.neo4j.driver.v1.tck.TCKTestUtil.asJavaType;
import static org.neo4j.driver.v1.tck.TCKTestUtil.boltValueAsCypherString;
import static org.neo4j.driver.v1.tck.TCKTestUtil.boltValuetoJavaObject;
import static org.neo4j.driver.v1.tck.TCKTestUtil.databaseRunning;
import static org.neo4j.driver.v1.tck.TCKTestUtil.getBoltValue;
import static org.neo4j.driver.v1.tck.TCKTestUtil.getListFromString;
import static org.neo4j.driver.v1.tck.TCKTestUtil.getListOfTypes;
import static org.neo4j.driver.v1.tck.TCKTestUtil.getMapOfTypes;
import static org.neo4j.driver.v1.tck.TCKTestUtil.getRandomString;


public class DriverComplianceSteps
{
    private MappedParametersRunner mappedParametersRunner;
    private StatementRunner statementRunner;
    private StringRunner stringRunner;
    private List<CypherStatementRunner> runners;
    private Object expectedJavaValue;
    private Value expectedBoltValue;
    private List<Object> listOfObjects;
    private Map<String,Object> mapOfObjects;

    @Before
    public void resetValues()
    {
        listOfObjects = new ArrayList<>();
        mapOfObjects = new HashMap<>();
        expectedJavaValue = null;
        expectedBoltValue = null;
        mappedParametersRunner = null;
        statementRunner = null;
        stringRunner = null;
        runners = new ArrayList<>();
    }

    @After
    public void print( Scenario sc )
    {
        if ( sc.isFailed() )
        {
            System.out.println( "Failed: " + sc.getName() );
        }
    }

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
    public void a_String_of_size(long size) throws Throwable
    {
        expectedJavaValue = getRandomString( size );
        expectedBoltValue = Values.value( expectedJavaValue );
    }

    @Given( "^a List of size (\\d+) and type ([^\"]*)$" )
    public void a_List_of_size_and_type_Type( long size, String type ) throws Throwable
    {
        expectedJavaValue = getListOfTypes( type, size );
        expectedBoltValue = Values.value( expectedJavaValue );
    }

    @Given( "^a Map of size (\\d+) and type ([^\"]*)$" )
    public void a_Map_of_size_and_type_Type( long size, String type) throws Throwable
    {
        expectedJavaValue = getMapOfTypes( type, size );
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

        runners.add(stringRunner);
        runners.add( mappedParametersRunner );
        runners.add( statementRunner );

        for ( CypherStatementRunner runner : runners)
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

    @Then( "^the result returned from the server should be a single record with a single value$" )
    public void result_should_be_of_single_record_with_a_single_value() throws Throwable
    {
        for( CypherStatementRunner runner : runners)
        {
            ResultCursor result = runner.result();
            assertTrue( result.single() );
        }
    }

    @Then( "^the value given in the result should be the same as what was sent" )
    public void result_should_be_equal_to_a_single_Type_of_Input( ) throws Throwable
    {
        for ( CypherStatementRunner runner : runners)
        {
            assertTrue( runner.result().single() );
            Value resultBoltValue = runner.result().record().get( 0 );
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
            Value receivedValue = runner.result().record().get( 0 );
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
            Value receivedValue = runner.result().record().get( 0 );
            Path path = receivedValue.asPath();
            Path expectedPath = expectedBoltValue.asPath();

            assertThat( path.start(), equalTo( expectedPath.start() ) );
            assertThat( path.end(), equalTo( expectedPath.end() ) );
            assertThat( path.nodes(), equalTo( expectedPath.nodes() ) );
            assertThat( path.relationships(), equalTo( expectedPath.relationships() ) );
            assertThat( receivedValue, equalTo( expectedBoltValue ) );
        }
    }
}
