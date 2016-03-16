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
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.tck.tck.util.runners.CypherStatementRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.MappedParametersRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.StringRunner;

import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.tck.Environment.driver;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.tck.Environment.runners;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.parseExpected;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.parseGiven;


public class CypherComplianceSteps
{
    @Given( "^init: (.*)$" )
    public void init_( String statement ) throws Throwable
    {
        try ( Session session = driver.session())
        {
            session.run( statement );
        }
    }

    @When( "^running: (.*)$" )
    public void running_( String statement ) throws Throwable
    {
        runners.add( new StringRunner( statement ).runCypherStatement() );
    }


    @When( "^running parametrized: (.*)$" )
    public void running_param_bar_match_a_r_b_where_r_foo_param_return_b( String statement, DataTable stringParam )
            throws Throwable
    {
        List<String> keys = stringParam.topCells();
        List<String> values = stringParam.diffableRows().get( 1 ).convertedRow;
        Map<String, Value> params = parseExpected( values, keys );
        runners.add( new MappedParametersRunner( statement, params ).runCypherStatement() );
    }

    @Then( "^result:$" )
    public void result(DataTable table) throws Throwable
    {
        for( CypherStatementRunner runner : runners)
        {
            StatementResult rc = runner.result();
            List<String> keys = table.topCells();
            Collection<Map> given = new ArrayList<>(  );
            Collection<Map> expected = new ArrayList<>(  );
            int i = 0;
            while ( rc.hasNext() )
            {
                Record record = rc.next();
                assertTrue( keys.size() == record.keys().size() );
                assertTrue( keys.containsAll( record.keys() ) );
                given.add( parseGiven( record.asMap( ofValue() ) ) );
                expected.add( parseExpected( table.diffableRows().get( i + 1 ).convertedRow, keys ) );
                i++;
            }
            assertTrue( equalRecords( expected, given ) );
        }
    }

    private boolean equalRecords( Collection<Map> one, Collection<Map> other )
    {
        if (one.size() != other.size() )
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
