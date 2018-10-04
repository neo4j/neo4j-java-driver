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
package org.neo4j.driver.v1.tck;

import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.tck.DriverComplianceIT.neo4j;
import static org.neo4j.driver.v1.util.Neo4jRunner.PASSWORD;
import static org.neo4j.driver.v1.util.Neo4jRunner.USER;

public class DriverAuthSteps
{
    private Driver driver;
    private Exception driverCreationError;

    @Given( "^a driver is configured with auth enabled and correct password is provided$" )
    public void aDriverIsConfiguredWithAuthEnabledAndCorrectPasswordIsProvided() throws Throwable
    {
        driver = GraphDatabase.driver( neo4j.uri(), AuthTokens.basic( USER, PASSWORD ) );
    }

    @Then( "^reading and writing to the database should be possible$" )
    public void readingAndWritingToTheDatabaseShouldBePossible() throws Throwable
    {
        try ( Session session = driver.session() )
        {
            session.run( "CREATE (:label1)" ).consume();
            session.run( "MATCH (n:label1) RETURN n" ).single();
        }
    }

    @Given( "^a driver is configured with auth enabled and the wrong password is provided$" )
    public void aDriverIsConfiguredWithAuthEnabledAndTheWrongPasswordIsProvided() throws Throwable
    {
        try
        {
            driver = GraphDatabase.driver( neo4j.uri(), AuthTokens.basic( USER,  "wrongPassword") );
        }
        catch ( Exception e )
        {
            driver = null;
            driverCreationError = e;
        }
    }

    @Then( "^reading and writing to the database should not be possible$" )
    public void readingAndWritingToTheDatabaseShouldNotBePossible() throws Throwable
    {
        Exception error = null;
        if ( driver != null )
        {
            try ( Session session = driver.session() )
            {
                session.run( "CREATE (:label1)" ).consume();
            }
            catch ( Exception e )
            {
                error = e;
            }
        }
        else
        {
            error = driverCreationError;
        }

        assertNotNull( "Exception should have been thrown", error );
        assertThat( error.getMessage(), startsWith( "The client is unauthorized due to authentication failure" ) );
    }

    @And( "^a `Protocol Error` is raised$" )
    public void aProtocolErrorIsRaised() throws Throwable
    {}
}
