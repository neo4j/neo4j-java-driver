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
package org.neo4j.docs.driver;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.StdIOCapture;
import org.neo4j.driver.v1.util.TestNeo4j;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.driver.v1.util.Neo4jRunner.PASSWORD;
import static org.neo4j.driver.v1.util.Neo4jRunner.USER;

public class ExamplesIT
{
    @ClassRule
    public static TestNeo4j neo4j = new TestNeo4j();

    private String uri;

    private int readInt( final String statement, final Value parameters )
    {
        try ( Session session = neo4j.driver().session() )
        {
            return session.readTransaction( new TransactionWork<Integer>()
            {
                @Override
                public Integer execute( Transaction tx )
                {
                    return tx.run( statement, parameters ).single().get( 0 ).asInt();
                }
            } );
        }
    }

    private int readInt( final String statement )
    {
        return readInt( statement, parameters() );
    }

    private void write( final String statement, final Value parameters )
    {
        try ( Session session = neo4j.driver().session() )
        {
            session.writeTransaction( new TransactionWork<Object>()
            {
                @Override
                public Object execute( Transaction tx )
                {
                    tx.run( statement, parameters );
                    return null;
                }
            } );
        }
    }

    private void write( String statement )
    {
        write( statement, parameters() );
    }

    private void clean()
    {
        write( "MATCH (a) DETACH DELETE a" );
    }

    private int personCount( String name )
    {
        return readInt( "MATCH (a:Person {name: $name}) RETURN count(a)", parameters( "name", name ) );
    }

    @Before
    public void setUp()
    {
        uri = neo4j.uri().toString();
        clean();
    }

    @Test
    public void testShouldRunAutocommitTransactionExample() throws Exception
    {
        // Given
        try ( AutocommitTransactionExample example = new AutocommitTransactionExample( uri, USER, PASSWORD ) )
        {
            // When
            example.addPerson( "Alice" );

            // Then
            assertThat( personCount( "Alice" ), greaterThan( 0 ) );
        }
    }

    @Test
    public void testShouldRunBasicAuthExample() throws Exception
    {
        // Given
        try ( BasicAuthExample example = new BasicAuthExample( uri, USER, PASSWORD ) )
        {
            // Then
            assertTrue( example.canConnect() );
        }
    }

    @Test
    public void testShouldRunConfigConnectionTimeoutExample() throws Exception
    {
        // Given
        try ( ConfigConnectionTimeoutExample example = new ConfigConnectionTimeoutExample( uri, USER, PASSWORD ) )
        {
            // Then
            assertThat( example, instanceOf( ConfigConnectionTimeoutExample.class ) );
        }
    }

    @Test
    public void testShouldRunConfigMaxRetryTimeExample() throws Exception
    {
        // Given
        try ( ConfigMaxRetryTimeExample example = new ConfigMaxRetryTimeExample( uri, USER, PASSWORD ) )
        {
            // Then
            assertThat( example, instanceOf( ConfigMaxRetryTimeExample.class ) );
        }
    }

    @Test
    public void testShouldRunConfigTrustExample() throws Exception
    {
        // Given
        try ( ConfigTrustExample example = new ConfigTrustExample( uri, USER, PASSWORD ) )
        {
            // Then
            assertThat( example, instanceOf( ConfigTrustExample.class ) );
        }
    }

    @Test
    public void testShouldRunConfigUnencryptedExample() throws Exception
    {
        // Given
        try ( ConfigUnencryptedExample example = new ConfigUnencryptedExample( uri, USER, PASSWORD ) )
        {
            // Then
            assertThat( example, instanceOf( ConfigUnencryptedExample.class ) );
        }
    }

    @Test
    public void testShouldRunCypherErrorExample() throws Exception
    {
        // Given
        try ( CypherErrorExample example = new CypherErrorExample( uri, USER, PASSWORD ) )
        {
            // When & Then
            StdIOCapture stdIO = new StdIOCapture();
            try ( AutoCloseable ignored = stdIO.capture() )
            {
                int employeeNumber = example.getEmployeeNumber( "Alice" );

                assertThat( employeeNumber, equalTo( -1 ) );
            }
            assertThat( stdIO.stderr(), equalTo( asList(
                    "Invalid input 'L': expected 't/T' (line 1, column 3 (offset: 2))",
                    "\"SELECT * FROM Employees WHERE name = $name\"",
                    "   ^" ) ) );
        }
    }

    @Test
    public void testShouldRunDriverLifecycleExample() throws Exception
    {
        // Given
        try ( DriverLifecycleExample example = new DriverLifecycleExample( uri, USER, PASSWORD ) )
        {
            // Then
            assertThat( example, instanceOf( DriverLifecycleExample.class ) );
        }
    }

    @Test
    public void testShouldRunHelloWorld() throws Exception
    {
        // Given
        try ( HelloWorldExample greeter = new HelloWorldExample( uri, USER, PASSWORD ) )
        {
            // When
            StdIOCapture stdIO = new StdIOCapture();

            try ( AutoCloseable ignored = stdIO.capture() )
            {
                greeter.printGreeting( "hello, world" );
            }

            // Then
            assertThat( stdIO.stdout().size(), equalTo( 1 ) );
            assertThat( stdIO.stdout().get( 0 ), containsString( "hello, world" ) );
        }
    }

    @Test
    public void testShouldRunReadWriteTransactionExample() throws Exception
    {
        // Given
        try ( ReadWriteTransactionExample example = new ReadWriteTransactionExample( uri, USER, PASSWORD ) )
        {
            // When
            long nodeID = example.addPerson( "Alice" );

            // Then
            assertThat( nodeID, greaterThanOrEqualTo( 0L ) );
        }
    }

    @Test
    public void testShouldRunResultConsumeExample() throws Exception
    {
        // Given
        write( "CREATE (a:Person {name: 'Alice'})" );
        write( "CREATE (a:Person {name: 'Bob'})" );
        try ( ResultConsumeExample example = new ResultConsumeExample( uri, USER, PASSWORD ) )
        {
            // When
            List<String> names = example.getPeople();

            // Then
            assertThat( names, equalTo( asList( "Alice", "Bob" ) ) );
        }
    }

    @Test
    public void testShouldRunResultRetainExample() throws Exception
    {
        // Given
        write( "CREATE (a:Person {name: 'Alice'})" );
        write( "CREATE (a:Person {name: 'Bob'})" );
        try ( ResultRetainExample example = new ResultRetainExample( uri, USER, PASSWORD ) )
        {
            // When
            example.addEmployees( "Acme" );

            // Then
            int employeeCount = readInt(
                    "MATCH (emp:Person)-[WORKS_FOR]->(com:Company) WHERE com.name = 'Acme' RETURN count(emp)" );
            assertThat( employeeCount, equalTo( 2 ) );
        }
    }

    @Test
    public void testShouldRunServiceUnavailableExample() throws Exception
    {
        // Given
        try ( ServiceUnavailableExample example = new ServiceUnavailableExample( uri, USER, PASSWORD ) )
        {
            try
            {
                // When
                neo4j.stopDb();

                // Then
                assertThat( example.addItem(), equalTo( false ) );
            }
            finally
            {
                neo4j.startDb();
            }
        }
    }

    @Test
    public void testShouldRunSessionExample() throws Exception
    {
        // Given
        try ( SessionExample example = new SessionExample( uri, USER, PASSWORD ) )
        {
            // When
            example.addPerson( "Alice" );

            // Then
            assertThat( example, instanceOf( SessionExample.class ) );
            assertThat( personCount( "Alice" ), greaterThan( 0 ) );
        }
    }

    @Test
    public void testShouldRunTransactionFunctionExample() throws Exception
    {
        // Given
        try ( TransactionFunctionExample example = new TransactionFunctionExample( uri, USER, PASSWORD ) )
        {
            // When
            example.addPerson( "Alice" );

            // Then
            assertThat( personCount( "Alice" ), greaterThan( 0 ) );
        }
    }
}
