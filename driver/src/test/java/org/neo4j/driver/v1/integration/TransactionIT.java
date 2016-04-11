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
package org.neo4j.driver.v1.integration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (n:FirstNode)" );
            tx.run( "CREATE (n:SecondNode)" );
            tx.success();
        }

        // Then the outcome of both statements should be visible
        StatementResult result = session.run( "MATCH (n) RETURN count(n)" );
        long nodes = result.single().get( "count(n)" ).asLong();
        assertThat( nodes, equalTo( 2L ) );
    }

    @Test
    public void shouldRunAndRollbackByDefault() throws Throwable
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "CREATE (n:FirstNode)" );
            tx.run( "CREATE (n:SecondNode)" );
        }

        // Then there should be no visible effect of the transaction
        StatementResult cursor = session.run( "MATCH (n) RETURN count(n)" );
        long nodes = cursor.single().get( "count(n)" ).asLong();
        assertThat( nodes, equalTo( 0L ) );
    }

    @Test
    public void shouldRetrieveResults() throws Throwable
    {
        // Given
        session.run( "CREATE (n {name:'Steve Brook'})" );

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            StatementResult res = tx.run( "MATCH (n) RETURN n.name" );

            // Then
            assertThat( res.single().get( "n.name" ).asString(), equalTo( "Steve Brook" ) );
        }
    }

    @Test
    public void shouldNotAllowSessionLevelStatementsWhenThereIsATransaction() throws Throwable
    {
        // Given
        session.beginTransaction();

        // Expect
        exception.expect( ClientException.class );

        // When
        session.run( "anything" );
    }

    @Test
    public void shouldBeClosedAfterRollback() throws Throwable
    {
        // When
        Transaction tx = session.beginTransaction();
        tx.close();

        // Then
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeClosedAfterCommit() throws Throwable
    {
        // When
        Transaction tx = session.beginTransaction();
        tx.success();
        tx.close();

        // Then
        assertFalse( tx.isOpen() );
    }

    @Test
    public void shouldBeOpenBeforeCommit() throws Throwable
    {
        // When
        Transaction tx = session.beginTransaction();

        // Then
        assertTrue( tx.isOpen() );
    }

    @Test
    public void shouldHandleNullParametersGracefully()
    {
        // When
        session.run("match (n) return count(n)", (Value)null);

        // Then
        // pass - no exception thrown

    }

    //See GH #146
    @Test
    public void shouldHandleFailureAfterClosingTransaction()
    {
        // GIVEN a successful query in a transaction
        Transaction tx = session.beginTransaction();
        StatementResult result = tx.run("CREATE (n) RETURN n");
        result.consume();
        tx.success();
        tx.close();

        // EXPECT
        exception.expect( ClientException.class );

        //WHEN running a malformed query in the original session
        session.run("CREAT (n) RETURN n").consume();
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldHandleNullRecordParameters() throws Throwable
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            Record params = null;
            tx.run( "CREATE (n:FirstNode)", params );
            tx.success();
        }

        // Then it wasn't the end of the world as we know it
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldHandleNullValueParameters() throws Throwable
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            Value params = null;
            tx.run( "CREATE (n:FirstNode)", params );
            tx.success();
        }

        // Then it wasn't the end of the world as we know it
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldHandleNullMapParameters() throws Throwable
    {
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            Map<String, Object> params = null;
            tx.run( "CREATE (n:FirstNode)", params );
            tx.success();
        }

        // Then it wasn't the end of the world as we know it
    }
}
