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
import cucumber.api.java.en.When;
import cucumber.runtime.table.DiffableRow;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.InputPosition;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.driver.v1.tck.tck.util.runners.CypherStatementRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.v1.tck.Environment.runners;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.getJavaValueIntAsLong;
import static org.neo4j.driver.v1.tck.tck.util.ResultParser.getJavaValueNormalInts;

public class DriverResultApiSteps
{
    List<ResultSummary> summaries;
    List<Statement> statements;

    @When( "^the `Statement Result` is consumed a `Result Summary` is returned$" )
    public void the_result_is_summerized() throws Throwable
    {
        summaries = new ArrayList<>();
        for ( CypherStatementRunner runner : runners )
        {
            summaries.add( runner.result().consume() );
        }
        ResultSummary first = summaries.get( 0 );
        for ( ResultSummary resultSummary : summaries )
        {
            assertThat( resultSummary, equalTo( first ) );
        }
    }

    @Then( "^the `Statement Result` is closed$" )
    public void theResultCursorIsFullyConsumed() throws Throwable
    {
        for ( CypherStatementRunner runner : runners )
        {
            assertThat( runner.result().list().isEmpty(), equalTo( true ) );
        }
    }

    @And( "^I request a `Statement` from the `Result Summary`$" )
    public void iRequestAStatementFromTheResultSummary() throws Throwable
    {
        statements = new ArrayList<>();
        for ( ResultSummary resultSummary : summaries )
        {
            statements.add( resultSummary.statement() );
        }
    }

    @Then( "^requesting the `Statement` as text should give: (.*)$" )
    public void requestingTheStatementAsTextShouldGive( String expected ) throws Throwable
    {
        for ( Statement statement : statements )
        {
            assertThat( statement.text(), equalTo( expected ) );
        }
    }

    @And( "^requesting the `Statement` parameter should give: (.*)$" )
    public void requestingTheStatementAsParameterShouldGiveNull( String expected ) throws Throwable
    {
        for ( int i = 0; i < statements.size(); i++ )
        {
            assertThat( statements.get( i ).parameters(), equalTo( runners.get( i ).parameters() ) );
        }
    }

    @Then( "^requesting `Counters` from `Result Summary` should give$" )
    public void iShouldGetUpdateStatisticsContaining( DataTable expectedStatistics ) throws Throwable
    {
        for ( ResultSummary resultSummary : summaries )
        {
            checkStatistics( resultSummary.counters(), tableToValueMap( expectedStatistics ) );
        }
    }

    @Then( "^requesting the `Statement Type` should give (.*)$" )
    public void theStatementTypeShouldBeType( String expectedType ) throws Throwable
    {
        StatementType expected;
        switch ( expectedType )
        {
        case "read only":
            expected = StatementType.READ_ONLY;
            break;
        case "write only":
            expected = StatementType.WRITE_ONLY;
            break;
        case "read write":
            expected = StatementType.READ_WRITE;
            break;
        case "schema write":
            expected = StatementType.SCHEMA_WRITE;
            break;
        default:
            throw new IllegalArgumentException( "Nu such type: " + expectedType );
        }
        for ( ResultSummary summary : summaries )
        {
            assertThat( summary.statementType(), equalTo( expected ) );
        }
    }

    @Then( "^the `Result Summary` has a `Plan`$" )
    public void theSummaryHasAPlan() throws Throwable
    {
        for ( ResultSummary summary : summaries )
        {
            assertThat( summary.hasPlan(), equalTo( true ) );
        }
    }

    @Then( "^the `Result Summary` does not have a `Plan`$" )
    public void theSummaryDoesNotHaveAPlan() throws Throwable
    {
        for ( ResultSummary summary : summaries )
        {
            assertThat( summary.hasPlan(), equalTo( false ) );
        }
    }

    @Then( "^the `Result Summary` has a `Profile`$" )
    public void theSummaryHasAProfile() throws Throwable
    {
        for ( ResultSummary summary : summaries )
        {
            assertThat( summary.hasProfile(), equalTo( true ) );
        }
    }

    @And( "^the `Result Summary` does not have a `Profile`$" )
    public void theSummaryDoesNotHaveAPriofile() throws Throwable
    {
        for ( ResultSummary summary : summaries )
        {
            assertThat( summary.hasProfile(), equalTo( false ) );
        }
    }

    @Then( "^requesting the `Plan` it contains$" )
    public void thePlanContains( DataTable expectedTable ) throws Throwable
    {
        HashMap<String,Object> expectedMap = tableToValueMap( expectedTable );
        for ( ResultSummary summary : summaries )
        {
            checkPlansValues( expectedMap, summary.plan() );
        }

    }

    @Then( "^requesting the `Profile` it contains:$" )
    public void theProfileContains( DataTable expectedTable ) throws Throwable
    {
        HashMap<String,Object> expectedMap = tableToValueMap( expectedTable );
        for ( ResultSummary summary : summaries )
        {
            checkPlansValues( expectedMap, summary.profile() );
        }
    }

    @And( "^the `Plan` also contains method calls for:$" )
    public void alsoContainsMethodCallsFor( DataTable expectedTable ) throws Throwable
    {
        HashMap<String,String> expectedMap = tableToMap( expectedTable );
        for ( ResultSummary summary : summaries )
        {
            checkPlanMethods( expectedMap, summary.plan() );
        }
    }

    @And( "^the `Profile` also contains method calls for:$" )
    public void profileAlsoContainsMethodCallsFor( DataTable expectedTable ) throws Throwable
    {
        HashMap<String,String> expectedMap = tableToMap( expectedTable );
        for ( ResultSummary summary : summaries )
        {
            checkPlanMethods( expectedMap, summary.profile() );
        }
    }

    @And( "^the `Result Summary` `Notifications` is empty$" )
    public void theSummaryDoesNotHaveAnyNotifications() throws Throwable
    {
        for ( ResultSummary summary : summaries )
        {
            assertThat( summary.notifications().size(), equalTo( 0 ) );
        }
    }

    @And( "^the `Result Summary` `Notifications` has one notification with$" )
    public void theSummaryHasNotifications( DataTable table ) throws Throwable
    {
        HashMap<String,Object> expected = new HashMap<>();
        for ( int i = 1; i < table.diffableRows().size(); i++ )
        {
            DiffableRow row = table.diffableRows().get( i );
            String key = row.convertedRow.get( 0 );
            Object value = getJavaValueNormalInts( row.convertedRow.get( 1 ) );
            expected.put( key, value );
        }
        for ( ResultSummary summary : summaries )
        {
            assertThat( summary.notifications().size() == 1, equalTo( true ) );
            Notification notification = summary.notifications().get( 0 );
            for ( String key : expected.keySet() )
            {
                switch ( key )
                {
                case "code":
                    compareNotificationValue( notification.code(), expected.get( key ) );
                    break;
                case "title":
                    compareNotificationValue( notification.title(), expected.get( key ) );
                    break;
                case "description":
                    compareNotificationValue( notification.description(), expected.get( key ) );
                    break;
                case "severity":
                    compareNotificationValue( notification.severity(), expected.get( key ) );
                    break;
                case "position":
                    Map<String,Object> expectedPosition = (Map<String,Object>) expected.get( key );
                    InputPosition position = notification.position();
                    for ( String positionKey : expectedPosition.keySet() )
                    {
                        switch ( positionKey )
                        {
                        case "offset":
                            compareNotificationValue( position.offset(), expectedPosition.get( positionKey ) );
                            break;
                        case "line":
                            compareNotificationValue( position.line(), expectedPosition.get( positionKey ) );
                            break;
                        case "column":
                            compareNotificationValue( position.column(), expectedPosition.get( positionKey ) );
                            break;
                        }
                    }
                    break;
                default:
                    throw new IllegalArgumentException( "No case for " + key );
                }



            }
        }
    }

    private void compareNotificationValue(Object given, Object expected )
    {
        assertThat( given, IsInstanceOf.instanceOf( expected.getClass() ) );
        assertThat( given, CoreMatchers.<Object>equalTo( expected ) );
    }

    public HashMap<String,Object> tableToValueMap( DataTable table )
    {
        HashMap<String,Object> map = new HashMap<>();
        for ( int i = 1; i < table.diffableRows().size(); i++ )
        {
            List<String> row = table.diffableRows().get( i ).convertedRow;
            map.put( row.get( 0 ), getJavaValueIntAsLong( row.get( 1 ) ) );
        }
        return map;
    }

    public HashMap<String,String> tableToMap( DataTable table )
    {
        HashMap<String,String> map = new HashMap<>();
        for ( int i = 1; i < table.diffableRows().size(); i++ )
        {
            List<String> row = table.diffableRows().get( i ).convertedRow;
            map.put( row.get( 0 ), row.get( 1 ) );
        }
        return map;
    }

    private void checkPlansValues( HashMap<String,Object> expected, Plan p )
    {
        for ( String key : expected.keySet() )
        {
            Object givenValue;
            switch ( key )
            {
            case "identifiers":
                givenValue = p.identifiers();
                break;
            case "operator type":
                givenValue = p.operatorType();
                break;
            case "db hits":
                assertThat( p, instanceOf( ProfiledPlan.class ) );
                givenValue = ((ProfiledPlan) p).dbHits();
                break;
            case "records":
                assertThat( p, instanceOf( ProfiledPlan.class ) );
                givenValue = ((ProfiledPlan) p).records();
                break;
            default:
                throw new IllegalArgumentException( "Nu such plan method: " + key );
            }
            assertThat( key + "- does not match", givenValue, equalTo( expected.get( key ) ) );
        }
    }

    private void checkPlanMethods( HashMap<String,String> expected, Plan plan )
    {
        for ( String key : expected.keySet() )
        {
            switch ( key )
            {
            case "children":
                assertThat( plan.children(), instanceOf( List.class ) );
                for ( Plan p : plan.children() )
                {
                    assertThat( p, instanceOf( Plan.class ) );
                    break;
                }
                break;
            case "arguments":
                assertThat( plan.arguments(), instanceOf( Map.class ) );
                for ( String k : plan.arguments().keySet() )
                {
                    assertThat( k, instanceOf( String.class ) );
                    assertThat( plan.arguments().get( k ), instanceOf( Value.class ) );
                    break;
                }
                break;
            default:
                throw new IllegalArgumentException( "There is no case for handeling method type: " + key );
            }
        }
    }

    private void checkStatistics( SummaryCounters statistics, Map<String,Object> expectedStatisticsMap )
    {
        for ( String key : expectedStatisticsMap.keySet() )
        {
            Object expectedValue = expectedStatisticsMap.get( key );
            Object givenValue;
            switch ( key )
            {
            case "nodes created":
                givenValue = statistics.nodesCreated();
                break;
            case "nodes deleted":
                givenValue = statistics.nodesDeleted();
                break;
            case "relationships created":
                givenValue = statistics.relationshipsCreated();
                break;
            case "relationships deleted":
                givenValue = statistics.relationshipsDeleted();
                break;
            case "properties set":
                givenValue = statistics.propertiesSet();
                break;
            case "labels added":
                givenValue = statistics.labelsAdded();
                break;
            case "labels removed":
                givenValue = statistics.labelsRemoved();
                break;
            case "indexes added":
                givenValue = statistics.indexesAdded();
                break;
            case "indexes removed":
                givenValue = statistics.indexesRemoved();
                break;
            case "constraints added":
                givenValue = statistics.constraintsAdded();
                break;
            case "constraints removed":
                givenValue = statistics.constraintsRemoved();
                break;
            case "contains updates":
                givenValue = statistics.containsUpdates();
                break;
            default:
                throw new IllegalArgumentException( "No function mapped to expression: " + key );
            }
            if ( givenValue instanceof Integer )
            {
                givenValue = Long.valueOf( (Integer) givenValue );
            }
            assertThat( key + " - did not match", givenValue, equalTo( expectedValue ) );
        }
    }
}
