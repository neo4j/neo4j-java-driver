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
import cucumber.api.java.en.Then;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.tck.tck.util.ResultParser;
import org.neo4j.driver.v1.tck.tck.util.runners.CypherStatementRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.tck.Environment.runners;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.parseGiven;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.parseStringValue;


public class DriverStatementResultAPISteps
{
    private void compareSingleRecord( Record record, List<String> keys, List<String> values )
    {

        assertThat( record.size(), equalTo( keys.size() ) );
        assertThat( ResultParser.parseExpected( values, keys ),
                equalTo( parseGiven( record.asMap( ofValue() ) ) ) );
    }

    @Then( "^using `Single` on `Statement Result` gives a `Record` containing:$" )
    public void usingSingleOnStatementReslutGivesARecordContaining( DataTable table ) throws Throwable
    {
        List<String> keys = table.diffableRows().get( 0 ).convertedRow;
        List<String> values = table.diffableRows().get( 1 ).convertedRow;

        for ( CypherStatementRunner runner : runners )
        {
            compareSingleRecord( runner.result().single(), keys, values );
        }
    }

    @Then( "^using `Single` on `Statement Result` throws exception:$" )
    public void usingSingleOnStatmentReslutThrowsException( DataTable table ) throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            Record single = null;
            boolean success = true;
            try
            {
                single = runner.result().single();
            }
            catch ( NoSuchRecordException e )
            {
                assertThat( e.getMessage(), startsWith( table.diffableRows().get( 1 ).convertedRow.get( 0 ) ) );
                success = false;
            }
            if ( success )
            {
                throw new Exception( "Excpected exception to be thrown but was not! Got: " + single );
            }
        }
    }

    @Then( "^using `Peek` on `Statement Result` fails$" )
    public void usingPeekOnStatmentReslutGivesNull() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            try
            {
                runner.result().peek();
                throw new Exception( "Expected NoSuchErrorException but did not get one." );
            }
            catch ( NoSuchRecordException ignore )
            {
            }
        }
    }

    @Then( "^using `Peek` on `Statement Result` gives a `Record` containing:$" )
    public void usingPeekOnStatmentReslutGivesARecord( DataTable table ) throws Throwable
    {
        List<String> keys = table.diffableRows().get( 0 ).convertedRow;
        List<String> values = table.diffableRows().get( 1 ).convertedRow;
        for ( CypherStatementRunner runner : runners )
        {
            compareSingleRecord( runner.result().peek(), keys, values );
        }
    }

    @And( "^using `Next` on `Statement Result` gives a `Record` containing:$" )
    public void usingNextOnStatementResultGivesARecordContaining( DataTable table ) throws Throwable
    {
        List<String> keys = table.diffableRows().get( 0 ).convertedRow;
        List<String> values = table.diffableRows().get( 1 ).convertedRow;

        for ( CypherStatementRunner runner : runners )
        {
            compareSingleRecord( runner.result().next(), keys, values );
        }
    }

    @And( "^using `Next` on `Statement Result` fails$" )
    public void usingNextOnStatementResultGivesNull() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            try
            {
                runner.result().next();
                throw new Exception( "Expected NoSuchErrorException but did not get one." );
            }
            catch ( NoSuchRecordException ignore )
            {
            }
        }
    }

    @Then( "^using `Keys` on `Statement Result` gives:$" )
    public void usingKeysOnStatementResultGives( List<String> data ) throws Throwable
    {
        HashSet<String> expected = new HashSet<>( data );
        expected.remove( data.get( 0 ) );
        for ( CypherStatementRunner runner : runners )
        {
            HashSet<String> keys = new HashSet<>( runner.result().keys() );
            assertThat( keys, equalTo( expected ) );
        }
    }

    @And( "^using `Next` on `Statement Result` gives a `Record`$" )
    public void usingNextOnStatementResultGivesARecord() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            Record single = runner.result().next();
            assertThat( single, instanceOf( Record.class ) );
        }
    }

    @Then( "^it is not possible to go back$" )
    public void itIsNotPossibleToGoBack() throws Throwable
    {
        //Move along
    }

    @And( "^using `List` on `Statement Result` gives a list of size (\\d+), the previous records are lost$" )
    public void usingListOnStatementResultGivesAListOfSizeThePreviousRecordsAreLost( int size ) throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            assertThat( runner.result().list().size(), equalTo( size ) );
        }
    }

    @Then( "^iterating through the `Statement Result` should follow the native code pattern$" )
    public void iteratingThroughTheStatementResultShouldFollowTheNativeCodePattern() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            while ( runner.result().hasNext() )
            {
                runner.result().next();
            }
        }
    }

    @Then( "^using `List` on `Statement Result` gives:$" )
    public void usingListOnStatementResultGives( DataTable table ) throws Throwable
    {
        List<String> keys = table.topCells();
        List<List<String>> expected = new ArrayList<>();
        for ( int i = 1; i < table.diffableRows().size(); i++ )
        {
            expected.add( table.diffableRows().get( i ).convertedRow );
        }
        for ( CypherStatementRunner runner : runners )
        {
            while ( runner.result().hasNext() )
            {
                List<Record> list = runner.result().list();
                while ( !list.isEmpty() )
                {
                    int size = list.size();
                    for ( List<String> expectedRecord : expected )
                    {
                        try
                        {
                            compareSingleRecord( list.get( 0 ), keys, expectedRecord );
                            list.remove( 0 );
                            break;
                        }
                        catch ( AssertionError ignore ) {}
                    }
                    if ( size == list.size() )
                    {
                        throw new Exception( "Actual does not match expected." );
                    }
                }
            }
        }
    }

    @Then( "^using `Keys` on the single record gives:$" )
    public void usingKeysOnTheSingleRecordGives( List<String> data ) throws Throwable
    {
        HashSet<String> expected = new HashSet<>( data );
        expected.remove( data.get( 0 ) );
        for ( CypherStatementRunner runner : runners )
        {
            HashSet<String> keys = new HashSet<>( runner.result().single().keys() );
            assertThat( keys, equalTo( expected ) );
        }
    }

    @Then( "^using `Values` on the single record gives:$" )
    public void usingValuesOnTheSingleRecordGives( List<String> data ) throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            List<Value> givens = runner.result().single().values();
            for ( int i = 1; i < data.size(); i++ )
            {
                Value given = parseGiven( givens.get( i - 1 ) );
                Value expected = parseStringValue( data.get( i ) );
                assertThat( given, equalTo( expected ) );
            }
        }
    }

    @Then( "^using `Get` with index (\\d+) on the single record gives:$" )
    public void usingGetWithIndexOnTheSingleRecordGives( int index, List<String> data ) throws Throwable
    {
        Value expected = parseStringValue( data.get( 1 ) );
        for ( CypherStatementRunner runner : runners )
        {
            Value given = parseGiven( runner.result().single().get( index ) );
            assertThat( given, equalTo( expected ) );
        }
    }

    @Then( "^using `Get` with key `(.*)` on the single record gives:$" )
    public void usingGetWithKeyNOnTheSingleRecordGives( String key, List<String> data ) throws Throwable
    {
        Value expected = parseStringValue( data.get( 1 ) );
        for ( CypherStatementRunner runner : runners )
        {
            Value given = parseGiven( runner.result().single().get( key ) );
            assertThat( given, equalTo( expected ) );
        }
    }

    @Then( "^using `Consume` on `StatementResult` gives `ResultSummary`$" )
    public void usingConsumeOnStatementResultGivesResultSummary() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            assertThat( runner.result().consume(), instanceOf( ResultSummary.class ) );
        }
    }

    @Then( "^using `Consume` on `StatementResult` multiple times gives the same `ResultSummary` each time$" )
    public void usingConsumeOnStatementResultMultipleTimesGivesTheSameResultSummaryEachTime() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            ResultSummary summary = runner.result().consume();
            assertThat( summary, instanceOf( ResultSummary.class ) );
            assertThat( summary.counters().nodesCreated(),
                    equalTo( runner.result().consume().counters().nodesCreated() ) );
        }
    }
}
