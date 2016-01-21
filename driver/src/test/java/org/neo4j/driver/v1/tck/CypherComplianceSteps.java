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
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.tck.tck.util.runners.CypherStatementRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.MappedParametersRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.StringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.tck.DriverComplianceIT.session;
import static org.neo4j.driver.v1.tck.Environment.runners;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.getMapFromString;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.getParametersFromListOfKeysAndValues;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.parseExpected;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.parseGiven;


public class CypherComplianceSteps
{
    @Given( "^init: (.*);$" )
    public void init_( String statement ) throws Throwable
    {
        session.run( statement );
    }

    @When( "^running: (.*);$" )
    public void running_( String statement ) throws Throwable
    {
        runners.add( new StringRunner( statement ).runCypherStatement() );
    }


    @When( "^running parametrized: (.*);$" )
    public void running_param_bar_match_a_r_b_where_r_foo_param_return_b( String statement, DataTable stringParam )
            throws Throwable
    {
        List<String> keys = stringParam.topCells();
        List<String> values = stringParam.diffableRows().get( 1 ).convertedRow;
        Map<String, Value> params = getParametersFromListOfKeysAndValues( keys, values );
        runners.add( new MappedParametersRunner( statement, params ).runCypherStatement() );
    }

    @Then( "^result should be ([^\"]*)\\(s\\)$" )
    public void result_should_be_a_type_containing(String type, DataTable table) throws Throwable
    {
        for( CypherStatementRunner runner : runners)
        {
            ResultCursor rc = runner.result();
            List<String> keys = table.topCells();
            Collection<Map> given = new ArrayList<>(  );
            Collection<Map> expected = new ArrayList<>(  );
            int i = 0;
            while ( rc.next() )
            {
                assertTrue( keys.size() == rc.record().keys().size() );
                assertTrue( keys.containsAll( rc.record().keys() ) );
                given.add( parseGiven( rc.record().asMap(), type ) );
                expected.add( parseExpected( table.diffableRows().get( i + 1 ).convertedRow, keys, type ) );
                i++;
            }
            assertTrue( equalRecords( expected, given) );
        }
    }

    @Then( "^result should be mixed: \"(.*)\"$" )
    public void result_should_be_mixed_a_node_b_node_l_integer( String stringTypes, DataTable table ) throws Throwable
    {
        for( CypherStatementRunner runner : runners)
        {
            Map<String, String> types = getMapFromString( stringTypes );
            ResultCursor rc = runner.result();
            List<String> keys = table.topCells();
            Collection<Map> given = new ArrayList<>(  );
            Collection<Map> expected = new ArrayList<>(  );
            int i = 0;
            while ( rc.next() )
            {
                assertTrue( keys.size() == rc.record().keys().size() );
                assertTrue( keys.containsAll( rc.record().keys() ) );
                Map<String,Value> tmpGiven = new HashMap<>(  );
                for ( String key : keys )
                {
                    tmpGiven.put( key, parseGiven( rc.record().asMap().get( key ), types.get( key ) ) );
                }
                given.add( tmpGiven );
                expected.add( parseExpected( table.diffableRows().get( i + 1 ).convertedRow, keys, types ) );
                i++;
            }
            assertTrue( equalRecords( expected, given ) );
        }
    }

    @Then( "^result should be empty$" )
    public void result_should_be_empty(DataTable table) throws Throwable
    {
        for (CypherStatementRunner runner : runners)
        {
            assertEquals( runner.result().list().size(), 0 );
        }

    }

    private boolean equalRecords( Collection<Map> one, Collection<Map> other )
    {
        if (one.size() != other.size() )
        {
            return false;
        }
        if (one.size() == 0)
        {
            return false;
        }
        for (Map c1 : one)
        {
            int otherSize = other.size();
            for (Map c2 : other)
            {
                if (c1.equals(c2))
                {
                    other.remove( c2 );
                    break;
                }
            }
            if (otherSize == other.size())
            {
                return false;
            }
        }
        return other.size() == 0;

    }
}
