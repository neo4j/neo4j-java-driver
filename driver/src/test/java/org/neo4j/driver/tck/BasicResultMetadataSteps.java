/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.tck;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.util.List;

import org.neo4j.driver.StatementType;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.driver.tck.CommonSteps.ctx;

public class BasicResultMetadataSteps
{
    @Given("^a `Result`$")
    public void a_Result() throws Throwable
    {
        ctx.result = ctx.session.run( "RETURN 1" );
    }

    @When("^I `summarize` it$")
    public void i_summarize_it() throws Throwable
    {
        ctx.summary = ctx.result.summarize();
    }

    @Then("^the `Result` is fully consumed$")
    public void the_Result_is_fully_consumed() throws Throwable
    {
        assertFalse( ctx.result.next() );
    }

    @Then("^I should get a `Result Summary`$")
    public void i_should_get_a_Result_Summary() throws Throwable
    {
        assertNotNull( ctx.summary );
    }

    @When("^I `summarize` a `Result`$")
    public void i_summarize_a_Result() throws Throwable
    {
        a_Result();
        i_summarize_it();
    }

    @Then("^the summary should contain `Statement Statistics` exposing <statistic>:$")
    public void the_summary_should_contain_Statement_Statistics_exposing_statistic(List<String> requiredStats) throws Throwable
    {
        for ( String stat : requiredStats )
        {
            if( stat.equals( "nodes created/deleted" ) )
            {
                assertEquals( 0, ctx.summary.nodesCreated() );
                assertEquals( 0, ctx.summary.nodesDeleted() );
                break;
            }
            else if( stat.equals( "relationships created/deleted" ))
            {
                assertEquals( 0, ctx.summary.relationshipsCreated() );
                assertEquals( 0, ctx.summary.relationshipsDeleted() );

            }
            else if( stat.equals( "properties set" ) )
            {
                assertEquals( 0, ctx.summary.propertiesSet() );

            }
            else if( stat.equals( "labels added/removed" ) )
            {
                assertEquals( 0, ctx.summary.labelsAdded() );
                assertEquals( 0, ctx.summary.labelsRemoved() );
            }
            else if( stat.equals( "indexes added/removed" ) )
            {
                assertEquals(0, ctx.summary.indexesAdded() );
                assertEquals(0, ctx.summary.indexesRemoved() );

            }
            else if( stat.equals( "constraints added/removed" ) )
            {
                assertEquals(0, ctx.summary.constraintsAdded() );
                assertEquals(0, ctx.summary.constraintsRemoved() );
            }
            else if( stat.equals( "contains updates" ) )
            {
                assertEquals(false, ctx.summary.containsUpdates() );
            }
            else if( !stat.equals( "statistic" ) )
            {
                fail("Unknown statistical variable: " + stat);
            }
        }
    }

    @Then("^the summary should contain a `Statement Type` <type> from this list:$")
    public void the_summary_should_contain_a_Statement_Type_type_from_this_list(List<String> statementType)
            throws Throwable
    {
        // The below code is written this way to enumerate all the specified statement types, ensuring this test
        // will fail if a new type is added to the TCK.
        boolean containedTypeFromList = false;
        StatementType found = ctx.summary.statementType();
        for ( String required : statementType )
        {
            if( required.equals("read only") )
            {
                containedTypeFromList = containedTypeFromList || found == StatementType.READ_ONLY;
            }
            else if( required.equals("read write") )
            {
                containedTypeFromList = containedTypeFromList || found == StatementType.READ_WRITE;
            }
            else if( required.equals("write only") )
            {
                containedTypeFromList = containedTypeFromList || found == StatementType.WRITE_ONLY;
            }
            else if( required.equals("schema write") )
            {
                containedTypeFromList = containedTypeFromList || found == StatementType.SCHEMA_WRITE;
            }
            else if( !required.equals("type" ) )
            {
                fail("Unknown statement type: " + required );
            }
        }

        if( !containedTypeFromList )
        {
            fail( ctx.summary.statementType().name() + " is not a statement type from the specification." );
        }

    }
}
