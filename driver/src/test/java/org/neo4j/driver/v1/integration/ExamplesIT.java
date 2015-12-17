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

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.StdIOCapture;
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
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
            // tag::minimum-snippet[]
            Driver driver = GraphDatabase.driver( "bolt://localhost" );
            Session session = driver.session();

            session.run( "CREATE (neo:Person {name:'Neo', age:23})" );

            ResultCursor result = session.run( "MATCH (p:Person) WHERE p.name = 'Neo' RETURN p.age" );
            while ( result.next() )
            {
                System.out.println( "Neo is " + result.value( "p.age" ).asInt() + " years old." );
            }

            session.close();
            driver.close();
            // end::minimum-snippet[]
        }

        // Then
        assertThat( stdIO.stdout(), equalTo( asList("Neo is 23 years old.") ) );
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
}
