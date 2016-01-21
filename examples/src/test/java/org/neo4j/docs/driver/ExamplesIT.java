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
package org.neo4j.docs.driver;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.StdIOCapture;
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * The tests below are examples that get pulled into the Driver Manual using the tags inside the tests.
 *
 * DO NOT add tests to this file that are not for that exact purpose.
 * DO NOT modify these tests without ensuring they remain consistent with the equivalent examples in other drivers
 */
public class ExamplesIT
{
    @Rule
    public TestNeo4j neo4j = new TestNeo4j();

    @Test
    public void minimalWorkingExample() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try ( AutoCloseable captured = stdIO.capture() )
        {
            MinimalWorkingExample.minimalWorkingExample();
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList( "Neo is 23 years old." ) ) );
    }

    @Test
    public void constructDriver() throws Throwable
    {
        Driver driver = Examples.constructDriver();

        // Then
        assertNotNull( driver );
        driver.close();
    }

    @Test
    public void configuration() throws Throwable
    {
        Driver driver = Examples.configuration();

        // Then
        assertNotNull( driver );
    }

    @Test
    public void statement() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try ( AutoCloseable captured = stdIO.capture();
                Driver driver = GraphDatabase.driver( "bolt://localhost" );
                Session session = driver.session() )
        {
            Examples.statement( session );
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList( "There were 1 the ones created." ) ) );
    }

    @Test
    public void statementWithoutParameters() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try ( AutoCloseable captured = stdIO.capture();
                Driver driver = GraphDatabase.driver( "bolt://localhost" );
                Session session = driver.session() )
        {
            Examples.statementWithoutParameters( session );
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList( "There were 1 the ones created." ) ) );
    }

    @Test
    public void resultCursor() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try ( AutoCloseable captured = stdIO.capture();
                Driver driver = GraphDatabase.driver( "bolt://localhost" );
                Session session = driver.session() )
        {
            session.run( "MATCH (n) DETACH DELETE n" );
            session.run( "CREATE (p:Person { name: 'The One', age:23 })" );

            Examples.resultCursor( session );
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList( "Record: 0", "  p.age = 23" ) ) );
    }

    @Test
    public void retainResultsForNestedQuerying() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try ( AutoCloseable captured = stdIO.capture();
                Driver driver = GraphDatabase.driver( "bolt://localhost" );
                Session session = driver.session() )
        {
            session.run( "MATCH (n) DETACH DELETE n" );
            session.run( "CREATE (p:Person { name: 'The One', age:23 })" );

            Examples.retainResultsForNestedQuerying( session );

            // Then
            int theOnes = session.run( "MATCH (:Person)-[:HAS_TRAIT]->() RETURN count(*)" ).peek().get( 0 ).asInt();
            assertEquals( 1, theOnes );
        }
    }

    @Test
    public void retainResultsForLaterProcessing() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try ( AutoCloseable captured = stdIO.capture();
                Driver driver = GraphDatabase.driver( "bolt://localhost" ); )
        {
            try ( Session setup = driver.session() )
            {
                setup.run( "MATCH (n) DETACH DELETE n" );
                setup.run( "CREATE (p:Person { name: 'The One', age:23 })" );
            }

            Examples.retainResultsForLaterProcessing( driver );
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList( "p.age = 23" ) ) );
    }

    @Test
    public void transactionCommit() throws Throwable
    {
        try ( Driver driver = GraphDatabase.driver( "bolt://localhost" );
                Session session = driver.session() )
        {
            session.run( "MATCH (n) DETACH DELETE n" );

            Examples.transactionCommit( session );

            // Then
            assertThat( session.run( "MATCH (p:Person) RETURN count(p)" ).peek().get( 0 ).asInt(), equalTo( 1 ) );
        }
    }

    @Test
    public void transactionRollback() throws Throwable
    {
        try ( Driver driver = GraphDatabase.driver( "bolt://localhost" );
                Session session = driver.session() )
        {
            session.run( "MATCH (n) DETACH DELETE n" );

            Examples.transactionRollback( session );

            // Then
            assertThat( session.run( "MATCH (p:Person) RETURN count(p)" ).peek().get( 0 ).asInt(), equalTo( 0 ) );
        }
    }

    @Test
    public void resultSummary() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try ( AutoCloseable captured = stdIO.capture();
                Driver driver = GraphDatabase.driver( "bolt://localhost" );
                Session session = driver.session() )
        {
            Examples.resultSummary( session );
        }

        assertThat( stdIO.stdout(), contains(
            equalTo( "READ_ONLY" ),
            containsString( "operatorType" )
        ));
    }

    @Test
    public void notifications() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try ( AutoCloseable captured = stdIO.capture();
                Driver driver = GraphDatabase.driver( "bolt://localhost" );
                Session session = driver.session() )
        {
            Examples.notifications( session );
        }

        assertThat( stdIO.stdout(), contains(
                containsString( "title=This query builds a cartesian product between disconnected patterns" )
        ));
    }

    @Test
    public void requireEncryption() throws Throwable
    {
        Driver driver = Examples.requireEncryption();

        // Then
        assertNotNull( driver );
        driver.close();
    }

    @Test
    public void trustOnFirstUse() throws Throwable
    {
        Driver driver = Examples.trustOnFirstUse();

        // Then
        assertNotNull( driver );
        driver.close();
    }

    @Test
    public void trustSignedCertificates() throws Throwable
    {
        Driver driver = Examples.trustSignedCertificates();

        // Then
        assertNotNull( driver );
        driver.close();
    }

}
