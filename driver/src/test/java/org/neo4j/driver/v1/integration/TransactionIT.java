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
import org.junit.rules.ExpectedException;

import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class TransactionIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldRunAndCommit() throws Throwable
    {
        // When
        try ( Transaction tx = session.newTransaction() )
        {
            tx.run( "CREATE (n:FirstNode)" );
            tx.run( "CREATE (n:SecondNode)" );
            tx.success();
        }

        // Then the outcome of both statements should be visible
        Result result = session.run( "MATCH (n) RETURN count(n)" );
        long nodes = result.single().get( "count(n)" ).javaLong();
        assertThat( nodes, equalTo( 2l ) );
    }

    @Test
    public void shouldRunAndRollbackByDefault() throws Throwable
    {
        // When
        try ( Transaction tx = session.newTransaction() )
        {
            tx.run( "CREATE (n:FirstNode)" );
            tx.run( "CREATE (n:SecondNode)" );
        }

        // Then there should be no visible effect of the transaction
        long nodes = session.run( "MATCH (n) RETURN count(n)" ).single().get( "count(n)" ).javaLong();
        assertThat( nodes, equalTo( 0l ) );
    }

    @Test
    public void shouldRetrieveResults() throws Throwable
    {
        // Given
        session.run( "CREATE (n {name:'Steve Brook'})" );

        // When
        try ( Transaction tx = session.newTransaction() )
        {
            Result res = tx.run( "MATCH (n) RETURN n.name" );

            // Then
            assertThat( res.single().get( "n.name" ).javaString(), equalTo( "Steve Brook" ) );
        }
    }

    @Test
    public void shouldNotAllowSessionLevelStatementsWhenThereIsATransaction() throws Throwable
    {
        // Given
        session.newTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        session.run( "anything" );
    }

    @Test
    public void shouldBeClosedAfterRollback() throws Throwable
    {
        // When
        Transaction tx = session.newTransaction();
        tx.close();

        // Then
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeClosedAfterCommit() throws Throwable
    {
        // When
        Transaction tx = session.newTransaction();
        tx.success();
        tx.close();

        // Then
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeOpenBeforeCommit() throws Throwable
    {
        // When
        Transaction tx = session.newTransaction();

        // Then
        assertTrue( tx.isOpen() );
    }

}
