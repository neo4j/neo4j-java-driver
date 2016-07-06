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

import java.util.UUID;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ErrorIT
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    @Test
    public void shouldThrowHelpfulSyntaxError() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Invalid input 'i'" );

        // When
        StatementResult result = session.run( "invalid statement" );
        result.consume();
    }

    @Test
    public void shouldNotAllowMoreTxAfterClientException() throws Throwable
    {
        // Given
        Transaction tx = session.beginTransaction();

        // And Given an error has occurred
        try { tx.run( "invalid" ).consume(); } catch ( ClientException e ) {}

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Cannot run more statements in this transaction, " +
                                 "because previous statements in the" );

        // When
        StatementResult cursor = tx.run( "RETURN 1" );
        cursor.single().get( "1" ).asInt();
    }

    @Test
    public void shouldAllowNewStatementAfterRecoverableError() throws Throwable
    {
        // Given an error has occurred
        try { session.run( "invalid" ).consume(); } catch ( ClientException e ) {}

        // When
        StatementResult cursor = session.run( "RETURN 1" );
        int val = cursor.single().get( "1" ).asInt();

        // Then
        assertThat( val, equalTo( 1 ) );
    }

    @Test
    public void shouldAllowNewTransactionAfterRecoverableError() throws Throwable
    {
        // Given an error has occurred in a prior transaction
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( "invalid" ).consume();
        }
        catch ( ClientException e ) {}

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            StatementResult cursor = tx.run( "RETURN 1" );
            int val = cursor.single().get( "1" ).asInt();

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

    @Test
    public void shouldHandleFailureAtCommitTime() throws Throwable
    {
        String label = UUID.randomUUID().toString();  // avoid clashes with other tests

        // given
        Transaction tx = session.beginTransaction();
        tx.run( "CREATE CONSTRAINT ON (a:`" + label + "`) ASSERT a.name IS UNIQUE" );
        tx.success();
        tx.close();

        // and
        tx = session.beginTransaction();
        tx.run( "CREATE INDEX ON :`" + label + "`(name)" );
        tx.success();

        // then expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Label '" + label + "' and property 'name' have a unique " +
                "constraint defined on them, so an index is already created that matches this." );

        // when
        tx.close();

    }

    @Test
    public void shouldGetHelpfulErrorWhenTryingToConnectToHttpPort() throws Throwable
    {
        // Given
        //the http server needs some time to start up
        Thread.sleep( 2000 );
        Driver driver = GraphDatabase.driver( "bolt://localhost:7474", Config.build().withEncryptionLevel(
                Config.EncryptionLevel.NONE ).toConfig());

        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
                                 "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)" );

        // When
        driver.session();
    }

}
