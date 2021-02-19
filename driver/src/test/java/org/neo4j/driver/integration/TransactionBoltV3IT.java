/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.util.EnabledOnNeo4jWith;
import org.neo4j.driver.util.DriverExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.time.Duration.ofMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.driver.internal.util.Neo4jFeature.BOLT_V3;
import static org.neo4j.driver.util.TestUtil.TX_TIMEOUT_TEST_TIMEOUT;
import static org.neo4j.driver.util.TestUtil.await;

@EnabledOnNeo4jWith( BOLT_V3 )
@ParallelizableIT
class TransactionBoltV3IT
{
    @RegisterExtension
    static final DriverExtension driver = new DriverExtension();

    @Test
    void shouldSetTransactionMetadata()
    {
        Map<String,Object> metadata = new HashMap<>();
        metadata.put( "key1", "value1" );
        metadata.put( "key2", 42L );
        metadata.put( "key3", false );

        TransactionConfig config = TransactionConfig.builder()
                .withMetadata( metadata )
                .build();

        try ( Transaction tx = driver.session().beginTransaction( config ) )
        {
            tx.run( "RETURN 1" ).consume();

            verifyTransactionMetadata( metadata );
        }
    }

    @Test
    void shouldSetTransactionMetadataAsync()
    {
        Map<String,Object> metadata = new HashMap<>();
        metadata.put( "hello", "world" );
        metadata.put( "key", ZonedDateTime.now() );

        TransactionConfig config = TransactionConfig.builder()
                .withMetadata( metadata )
                .build();

        CompletionStage<AsyncTransaction> txFuture = driver.asyncSession().beginTransactionAsync( config )
                .thenCompose( tx -> tx.runAsync( "RETURN 1" )
                        .thenCompose( ResultCursor::consumeAsync )
                        .thenApply( ignore -> tx ) );

        AsyncTransaction transaction = await( txFuture );
        try
        {
            verifyTransactionMetadata( metadata );
        }
        finally
        {
            await( transaction.rollbackAsync() );
        }
    }

    @Test
    void shouldSetTransactionTimeout()
    {
        // create a dummy node
        Session session = driver.session();
        session.run( "CREATE (:Node)" ).consume();

        try ( Session otherSession = driver.driver().session() )
        {
            try ( Transaction otherTx = otherSession.beginTransaction() )
            {
                // lock dummy node but keep the transaction open
                otherTx.run( "MATCH (n:Node) SET n.prop = 1" ).consume();

                assertTimeoutPreemptively( TX_TIMEOUT_TEST_TIMEOUT, () -> {
                    TransactionConfig config = TransactionConfig.builder()
                            .withTimeout( ofMillis( 1 ) )
                            .build();

                    // start a new transaction with timeout and try to update the locked dummy node
                    Exception error = assertThrows( Exception.class, () ->
                    {
                        try ( Transaction tx = session.beginTransaction( config ) )
                        {
                            tx.run( "MATCH (n:Node) SET n.prop = 2" );
                            tx.commit();
                        }
                    } );

                    verifyValidException( error );
                } );
            }
        }
    }

    @Test
    void shouldSetTransactionTimeoutAsync()
    {
        // create a dummy node
        Session session = driver.session();
        AsyncSession asyncSession = driver.asyncSession();

        session.run( "CREATE (:Node)" ).consume();

        try ( Session otherSession = driver.driver().session() )
        {
            try ( Transaction otherTx = otherSession.beginTransaction() )
            {
                // lock dummy node but keep the transaction open
                otherTx.run( "MATCH (n:Node) SET n.prop = 1" ).consume();

                assertTimeoutPreemptively( TX_TIMEOUT_TEST_TIMEOUT, () -> {
                    TransactionConfig config = TransactionConfig.builder()
                            .withTimeout( ofMillis( 1 ) )
                            .build();

                    // start a new transaction with timeout and try to update the locked dummy node
                    CompletionStage<Void> txCommitFuture = asyncSession.beginTransactionAsync( config )
                            .thenCompose( tx -> tx.runAsync( "MATCH (n:Node) SET n.prop = 2" )
                                    .thenCompose( ignore -> tx.commitAsync() ) );

                    Exception error = assertThrows( Exception.class, () -> await( txCommitFuture ) );

                    verifyValidException( error );
                } );
            }
        }
    }


    private static void verifyValidException( Exception error )
    {
        // Server 4.1 corrected this exception to ClientException. Testing either here for compatibility
        if ( error instanceof TransientException || error instanceof ClientException )
        {
            assertThat( error.getMessage(), containsString( "terminated" ) );
        }
        else
        {
            fail( "Expected either a TransientException or ClientException", error );
        }
    }

    private static void verifyTransactionMetadata( Map<String,Object> metadata )
    {
        try ( Session session = driver.driver().session() )
        {
            Result result = session.run( "CALL dbms.listTransactions()" );

            Map<String,Object> receivedMetadata = result.list()
                    .stream()
                    .map( record -> record.get( "metaData" ) )
                    .map( Value::asMap )
                    .filter( map -> !map.isEmpty() )
                    .findFirst()
                    .orElseThrow( IllegalStateException::new );

            assertEquals( metadata, receivedMetadata );
        }
    }
}
