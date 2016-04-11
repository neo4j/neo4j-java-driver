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

import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;

import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.v1.tck.Environment.driver;

public class ErrorReportingSteps
{
    TransactionRunner transactionRunner = null;
    Exception exception = null;
    Driver smallDriver = null;
    List<Session> storedSessions;

    class TransactionRunner
    {
        private Session session;
        private Transaction tx;

        public TransactionRunner( Session session )
        {
            this.tx = null;
            this.session = session;
        }

        public void beginTransaction()
        {
            this.tx = this.session.beginTransaction();
        }

        public void closeTransaction()
        {
            this.tx.close();
        }

        private void close()
        {
            if ( this.tx != null )
            {
                closeTransaction();
            }
            try
            {
                this.session.close();
            }
            catch ( Exception ignore ) {}
        }
    }

    @Before( "@error_reporting" )
    public void setUp()
    {
        transactionRunner = null;
        exception = null;
        storedSessions = new ArrayList<>();
    }

    @After( "@error_reporting" )
    public void tearDown()
    {
        if ( transactionRunner != null )
        {transactionRunner.close();}
        if ( smallDriver != null )
        { smallDriver.close();}
        for ( Session s : storedSessions )
        {
            s.close();
        }
    }

    @Given( "^I have a driver$" )
    public void iHaveADriver() throws Throwable
    {
    }

    @When( "^I start a `Transaction` through a session$" )
    public void iStartATransactionThroughASession() throws Throwable
    {
        transactionRunner = new TransactionRunner( driver.session() );
        transactionRunner.beginTransaction();
    }

    @And( "^`run` a query with that same session without closing the transaction first$" )
    public void runAQueryWithThatSameSessionWithoutClosingTheTransactionFirstThrowsAClientException() throws Throwable
    {
        try
        {
            transactionRunner.session.run( "CREATE (:n)" );
        }
        catch ( Exception e )
        {
            transactionRunner.closeTransaction();
            exception = e;
        }
    }

    @And( "^I start a new `Transaction` with the same session before closing the previous$" )
    public void iStartANewTransactionWithTheSameSessionBeforeClosingThePreviousThrowsAClientException() throws Throwable
    {
        try
        {
            transactionRunner.beginTransaction();
        }
        catch ( Exception e )
        {
            transactionRunner.closeTransaction();
            exception = e;
        }
    }

    @When( "^I run a non valid cypher statement$" )
    public void iRunANonValidCypherStatementItThrowsAnClientException() throws Throwable
    {
        try ( Session session = driver.session() )
        {
            session.run( "CRETE (n)" ).consume();
        }
        catch ( Exception e )
        {
            exception = e;
        }
    }

    @When( "^I set up a driver to an incorrect port" )
    public void iSetUpADriverToAnIncorrectPort() throws Throwable
    {
        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:7777" ) )
        {
            driver.session();
        }
        catch ( Exception e )
        {
            exception = e;
        }
    }

    @Then( "^it throws a `ClientException`$" )
    public void itThrowsAnClientException( List<String> data ) throws Throwable
    {
        assertNotNull( exception );
        assertThat( exception, instanceOf( ClientException.class ) );
        assertThat( exception.getMessage(), startsWith( data.get( 1 ) ) );
    }

    @And( "^I get a session from the driver and close the driver$" )
    public void iGetASessionFromTheDriver() throws Throwable
    {
        try ( Driver driver = GraphDatabase.driver( "bolt://localhost" ) )
        {
            transactionRunner = new TransactionRunner( driver.session() );
        }
    }

    @When( "^I get a session from the driver and close the session$" )
    public void iGetASessionFromTheDriverAndCloseTheSession() throws Throwable
    {
        transactionRunner = new TransactionRunner( driver.session() );
        transactionRunner.session.close();
    }

    @And( "^try to run a cypher statement$" )
    public void tryToRunACypherStatement() throws Throwable
    {
        try
        {
            transactionRunner.session.run( "CREATE (:n)" );
        }
        catch ( Exception e )
        {
            exception = e;
        }
    }

    @When( "^I set up a driver with wrong scheme$" )
    public void iSetUpADriverToAnIncorrectScheme() throws Throwable
    {
        try ( Driver driver = GraphDatabase.driver( "wrong://localhost:7687" ) )
        {
            driver.session();
        }
        catch ( Exception e )
        {
            exception = e;
        }
    }

    @Given( "^I have a driver with fixed pool size of (\\d+)$" )
    public void iHaveADriverWithFixedPoolSizeOf( int poolSize ) throws Throwable
    {
        smallDriver = GraphDatabase.driver( "bolt://localhost", Config.build().withMaxSessions( poolSize ).toConfig() );
    }

    @And( "^I try to get a session$" )
    public void iTryToGetAnotherSession() throws Throwable
    {
        try ( Session session = smallDriver.session() )
        {
            session.run( "CREATE (:n)" );
        }
        catch ( Exception e )
        {
            exception = e;
        }
    }

    @And( "^I close the session$" )
    public void iCloseTheSession() throws Throwable
    {
        transactionRunner.session.close();
    }

    @Then( "^I get no exception$" )
    public void iGetNoException() throws Throwable
    {
        assertNull( exception );
    }

    @And( "^I store a session$" )
    public void iStoreASession() throws Throwable
    {
        storedSessions.add( smallDriver.session() );
    }
}