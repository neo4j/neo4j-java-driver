/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.List;

import org.neo4j.driver.internal.spi.Logger;
import org.neo4j.driver.internal.spi.Logging;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Notification;
import org.neo4j.driver.v1.Pair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.ResultSummary;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.StdIOCapture;
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Config.TlsAuthenticationConfig.usingKnownCerts;
import static org.neo4j.driver.v1.Config.TlsAuthenticationConfig.usingTrustedCert;

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
    public void minimumViableSnippet() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try( AutoCloseable captured = stdIO.capture() )
        {
            ImportExample.main();
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList("Neo is 23 years old.") ) );
    }

    @Test
    public void constructDriver() throws Throwable
    {
        // tag::construct-driver[]
        Driver driver = GraphDatabase.driver( "bolt://localhost" );
        // end::construct-driver[]

        // Then
        assertNotNull( driver );
    }

    @Test
    public void configuration() throws Throwable
    {
        // tag::configuration[]
        Driver driver = GraphDatabase.driver( "bolt://localhost",
                Config.build()
                        .withLogging( new MyLogging() )
                        .toConfig());
        // end::configuration[]

        // Then
        assertNotNull( driver );
    }

    @Test
    public void statement() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try( AutoCloseable captured = stdIO.capture();
             Driver driver = GraphDatabase.driver( "bolt://localhost" );
             Session session = driver.session() )
        {
            // tag::statement[]
            ResultCursor result = session.run( "CREATE (p:Person { name: {name} })", Values.parameters( "name", "The One" ) );

            int theOnesCreated = result.summarize().updateStatistics().nodesCreated();
            System.out.println("There were " + theOnesCreated + " the ones created.");
            // end::statement[]
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList("There were 1 the ones created.") ) );
    }

    @Test
    public void statementWithoutParameters() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try( AutoCloseable captured = stdIO.capture();
             Driver driver = GraphDatabase.driver( "bolt://localhost" );
             Session session = driver.session() )
        {
            // tag::statement-without-parameters[]
            ResultCursor result = session.run( "CREATE (p:Person { name: 'The One' })" );

            int theOnesCreated = result.summarize().updateStatistics().nodesCreated();
            System.out.println("There were " + theOnesCreated + " the ones created.");
            // end::statement-without-parameters[]
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList("There were 1 the ones created.") ) );
    }

    @Test
    public void resultCursor() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try( AutoCloseable captured = stdIO.capture();
             Driver driver = GraphDatabase.driver( "bolt://localhost" );
             Session session = driver.session() )
        {
            session.run( "MATCH (n) DETACH DELETE n" );
            session.run( "CREATE (p:Person { name: 'The One', age:23 })" );

            // tag::result-cursor[]
            ResultCursor result = session.run( "MATCH (p:Person { name: {name} }) RETURN p.age",
                    Values.parameters( "name", "The One" ) );

            while( result.next() )
            {
                System.out.println("Record: " + result.position() );
                for ( Pair<String,Value> fieldInRecord : result.fields() )
                {
                    System.out.println( "  " + fieldInRecord.key() + " = " + fieldInRecord.value() );
                }
            }
            // end::result-cursor[]
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList("Record: 0","  p.age = 23 :: INTEGER") ) );
    }

    @Test
    public void retainResultsForNestedQuerying() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try( AutoCloseable captured = stdIO.capture();
             Driver driver = GraphDatabase.driver( "bolt://localhost" );
             Session session = driver.session() )
        {
            session.run( "MATCH (n) DETACH DELETE n" );
            session.run( "CREATE (p:Person { name: 'The One', age:23 })" );

            // tag::retain-result-query[]
            ResultCursor result = session.run( "MATCH (p:Person { name: {name} }) RETURN id(p)",
                    Values.parameters( "name", "The One" ) );

            for ( Record record : result.list() )
            {
                session.run( "MATCH (p) WHERE id(p) = {id} " +
                             "CREATE (p)-[:HAS_TRAIT]->(:Trait {type:'Immortal'})",
                        Values.parameters( "id", record.value( "id(p)" ) ) );
            }
            // end::retain-result-query[]
        }
    }

    @Test
    public void retainResultsForLaterProcessing() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try( AutoCloseable captured = stdIO.capture();
             Driver driver = GraphDatabase.driver( "bolt://localhost" ); )
        {
            try( Session setup = driver.session() )
            {
                setup.run( "MATCH (n) DETACH DELETE n" );
                setup.run( "CREATE (p:Person { name: 'The One', age:23 })" );
            }

            // tag::retain-result-process[]
            Session session = driver.session();

            ResultCursor result = session.run( "MATCH (p:Person { name: {name} }) RETURN p.age",
                    Values.parameters( "name", "The One" ) );

            List<Record> records = result.list();

            session.close();

            for ( Record record : records )
            {
                for ( Pair<String,Value> fieldInRecord : record.fields() )
                {
                    System.out.println( fieldInRecord.key() + " = " + fieldInRecord.value() );
                }
            }
            // end::retain-result-process[]
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList("p.age = 23 :: INTEGER") ) );
    }


    @Test
    public void transactionCommit() throws Throwable
    {
        try( Driver driver = GraphDatabase.driver( "bolt://localhost" );
             Session session = driver.session() )
        {
            session.run("MATCH (n) DETACH DELETE n");

            // tag::transaction-commit[]
            try( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (p:Person { name: 'The One' })" );
                tx.success();
            }
            // end::transaction-commit[]

            // Then
            assertThat( session.run( "MATCH (p:Person) RETURN count(p)" ).peek().value( 0 ).asInt(), equalTo( 1 ) );
        }
    }

    @Test
    public void transactionRollback() throws Throwable
    {
        try( Driver driver = GraphDatabase.driver( "bolt://localhost" );
             Session session = driver.session() )
        {
            session.run("MATCH (n) DETACH DELETE n");

            // tag::transaction-rollback[]
            try( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (p:Person { name: 'The One' })" );
                tx.failure();
            }
            // end::transaction-rollback[]

            // Then
            assertThat( session.run( "MATCH (p:Person) RETURN count(p)" ).peek().value( 0 ).asInt(), equalTo( 0 ) );
        }
    }

    @Test
    public void resultSummary() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try( AutoCloseable captured = stdIO.capture();
             Driver driver = GraphDatabase.driver( "bolt://localhost" );
             Session session = driver.session() )
        {
            // tag::result-summary-query-profile[]
            ResultCursor result = session.run( "PROFILE MATCH (p:Person { name: {name} }) RETURN id(p)",
                    Values.parameters( "name", "The One" ) );

            ResultSummary summary = result.summarize();

            System.out.println( summary.statementType() );
            System.out.println( summary.profile() );
            // end::result-summary-query-profile[]
        }
    }

    @Test
    public void notifications() throws Throwable
    {
        StdIOCapture stdIO = new StdIOCapture();
        try( AutoCloseable captured = stdIO.capture();
             Driver driver = GraphDatabase.driver( "bolt://localhost" );
             Session session = driver.session() )
        {
            // tag::result-summary-notifications[]
            ResultSummary summary = session.run( "EXPLAIN MATCH (a), (b) RETURN a,b" ).summarize();

            for ( Notification notification : summary.notifications() )
            {
                System.out.println(notification);
            }
            // end::result-summary-notifications[]
        }
    }

    @Test
    public void requireEncryption() throws Throwable
    {
        // tag::tls-require-encryption[]
        Driver driver = GraphDatabase.driver( "bolt://localhost", Config.build()
                .withTlsEnabled( true )
                .toConfig() );
        // end::tls-require-encryption[]
        driver.close();
    }

    @Test
    public void trustOnFirstUse() throws Throwable
    {
        // tag::tls-trust-on-first-use[]
        Driver driver = GraphDatabase.driver( "bolt://localhost", Config.build()
                .withTlsEnabled( true )
                .withTlsAuthConfig( usingKnownCerts( new File("/path/to/neo4j_known_hosts") ) )
                .toConfig() );
        // end::tls-trust-on-first-use[]
        driver.close();
    }

    @Test
    public void trustSignedCertificates() throws Throwable
    {
        // tag::tls-signed[]
        Driver driver = GraphDatabase.driver( "bolt://localhost", Config.build()
                .withTlsEnabled( true )
                .withTlsAuthConfig( usingTrustedCert( new File("/path/to/ca-certificate.pem") ) )
                .toConfig() );
        // end::tls-signed[]
        driver.close();
    }

    private class MyLogging implements Logging
    {
        @Override
        public Logger getLog( String name )
        {
            return null;
        }
    }
}
