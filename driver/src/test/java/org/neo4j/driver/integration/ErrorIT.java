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
package org.neo4j.driver.integration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.util.TestSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.startsWith;

public class ErrorIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TestSession session = new TestSession();

    @Test
    public void shouldThrowHelpfulSyntaxError() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Invalid input 'i': expected <init> (line 1, column 1 (offset: 0))\n" +
                                 "\"invalid statement\"\n" +
                                 " ^" );

        // When
        session.run( "invalid statement" );
    }

    @Test
    public void shouldNotAllowMoreTxAfterClientException() throws Throwable
    {
        // Given
        Transaction tx = session.newTransaction();

        // And Given an error has occurred
        try { tx.run( "invalid" ); } catch ( ClientException e ) {}

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( startsWith( "Cannot run more statements in this transaction, " +
                                             "because previous statements in the" ) );

        // When
        tx.run( "RETURN 1" ).single().get( "1" ).javaInteger();
    }

    @Test
    public void shouldAllowNewStatementAfterRecoverableError() throws Throwable
    {
        // Given an error has occurred
        try { session.run( "invalid" ); } catch ( ClientException e ) {}

        // When
        int val = session.run( "RETURN 1" ).single().get( "1" ).javaInteger();

        // Then
        assertThat( val, equalTo( 1 ) );
    }

    @Test
    public void shouldAllowNewTransactionAfterRecoverableError() throws Throwable
    {
        // Given an error has occurred in a prior transaction
        try ( Transaction tx = session.newTransaction() )
        {
            tx.run( "invalid" );
        }
        catch ( ClientException e ) {}

        // When
        try ( Transaction tx = session.newTransaction() )
        {
            int val = tx.run( "RETURN 1" ).single().get( "1" ).javaInteger();

            // Then
            assertThat( val, equalTo( 1 ) );
        }
    }

    @Test
    public void shouldExplainConnectionError() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Unable to connect to 'localhost' on port 7777, ensure the database is running " +
                                 "and that there is a working network connection to it." );

        // When
        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:7777" ) )
        {
            driver.session();
        }
    }

}
