/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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


import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.Neo4jSettings;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.tck.DriverComplianceIT.neo4j;

public class DriverAuthSteps
{
    private Driver driver;
    private File tempDir;
    private Exception driverCreationError;

    @Before( "@auth" )
    public void setUp() throws IOException
    {
        tempDir = Files.createTempDirectory("dbms").toFile();
        tempDir.deleteOnExit();
    }

    @After( "@auth" )
    public void reset()
    {

        try
        {
            if (driver != null)
            {
                driver.close();
            }
            neo4j.restartDb();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( "Failed to reset database" );
        }
    }

    @Given( "^a driver is configured with auth enabled and correct password is provided$" )
    public void aDriverIsConfiguredWithAuthEnabledAndCorrectPasswordIsProvided() throws Throwable
    {
        driver = configureCredentials( "neo4j", "neo4j", "password" );
    }

    @Then( "^reading and writing to the database should be possible$" )
    public void readingAndWritingToTheDatabaseShouldBePossible() throws Throwable
    {
        Session session = driver.session();
        session.run( "CREATE (:label1)" ).consume();
        session.run( "MATCH (n:label1) RETURN n" ).single();
        session.close();
    }

    @Given( "^a driver is configured with auth enabled and the wrong password is provided$" )
    public void aDriverIsConfiguredWithAuthEnabledAndTheWrongPasswordIsProvided() throws Throwable
    {
        driver = configureCredentials( "neo4j", "neo4j", "password" );
        driver.close();

        try
        {
            driver = GraphDatabase.driver( neo4j.uri(), new InternalAuthToken(
                    parameters(
                            "scheme", "basic",
                            "principal", "neo4j",
                            "credentials", "wrong" ).asMap( ofValue() ) ) );
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

    private Driver configureCredentials( String name, String oldPassword, String newPassword ) throws Exception
    {
        neo4j.restartDb( Neo4jSettings.TEST_SETTINGS
                .updateWith( Neo4jSettings.AUTH_ENABLED, "true" )
                .updateWith( Neo4jSettings.DATA_DIR, tempDir.getAbsolutePath().replace("\\", "/") ));

        Driver driver = GraphDatabase.driver( neo4j.uri(), new InternalAuthToken(
                parameters(
                        "scheme", "basic",
                        "principal", name,
                        "credentials", oldPassword,
                        "new_credentials", newPassword ).asMap( ofValue() ) ) );
        try(Session session = driver.session())
        {
            session.run( "RETURN 1" );
        }
        return driver;
    }
}
