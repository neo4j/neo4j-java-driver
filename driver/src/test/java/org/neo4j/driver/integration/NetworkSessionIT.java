/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Record;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.async.AsyncTransactionWork;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.async.EventLoopGroupFactory;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.util.Matchers.blockingOperationInEventLoopError;
import static org.neo4j.driver.util.TestUtil.await;

/**
 * We leave the question whether we want to let {@link NetworkSession} both implements blocking session and async session or not later.
 * But as how it is right not, here are some tests for using mixes blocking and async API.
 */
@ParallelizableIT
class NetworkSessionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private NetworkSession session;

    @BeforeEach
    void setUp()
    {
        session = newSession();
    }

    @AfterEach
    void tearDown()
    {
        session.closeAsync();
    }

    private NetworkSession newSession()
    {
        return (NetworkSession) neo4j.driver().session();
    }

    @Test
    void shouldBePossibleToMixRunAsyncAndBlockingSessionClose()
    {
        long nodeCount = 5_000;

        try ( NetworkSession session = newSession() )
        {
            session.runAsync( "UNWIND range(1, " + nodeCount + ") AS x CREATE (n:AsyncNode {x: x}) RETURN n" );
        }

        assertEquals( nodeCount, countNodesByLabel( "AsyncNode" ) );
    }

    @Test
    void shouldFailToExecuteBlockingRunInAsyncTransactionFunction()
    {
        AsyncTransactionWork<CompletionStage<Void>> completionStageTransactionWork = tx -> {
            if ( EventLoopGroupFactory.isEventLoopThread( Thread.currentThread() ) )
            {
                IllegalStateException e = assertThrows( IllegalStateException.class,
                        () -> ((ExplicitTransaction) tx).run( "UNWIND range(1, 10000) AS x CREATE (n:AsyncNode {x: x}) RETURN n" ) );

                assertThat( e, is( blockingOperationInEventLoopError() ) );
            }
            return completedFuture( null );
        };

        CompletionStage<Void> result = session.readTransactionAsync( completionStageTransactionWork );
        assertNull( await( result ) );
    }

    @Test
    void shouldFailToExecuteBlockingRunChainedWithAsyncRun()
    {
        CompletionStage<Void> result = session.runAsync( "RETURN 1" ).thenCompose( StatementResultCursor::singleAsync ).thenApply( record -> {
            if ( EventLoopGroupFactory.isEventLoopThread( Thread.currentThread() ) )
            {
                IllegalStateException e =
                        assertThrows( IllegalStateException.class, () -> session.run( "RETURN $x", parameters( "x", record.get( 0 ).asInt() ) ) );

                assertThat( e, is( blockingOperationInEventLoopError() ) );
            }
            return null;
        } );

        assertNull( await( result ) );
    }

    @Test
    void shouldAllowBlockingOperationInCommonPoolWhenChaining()
    {
        CompletionStage<Node> nodeStage = session.runAsync( "RETURN 42 AS value" ).thenCompose( StatementResultCursor::singleAsync )
                // move execution to ForkJoinPool.commonPool()
                .thenApplyAsync( record -> session.run( "CREATE (n:Node {value: $value}) RETURN n", record ) ).thenApply( StatementResult::single ).thenApply(
                        record -> record.get( 0 ).asNode() );

        Node node = await( nodeStage );

        assertEquals( 42, node.get( "value" ).asInt() );
        assertEquals( 1, countNodesByLabel( "Node" ) );
    }

    private void runNestedQueries( StatementResultCursor inputCursor, List<CompletionStage<Record>> stages,
            CompletableFuture<List<CompletionStage<Record>>> resultFuture )
    {
        final CompletionStage<Record> recordResponse = inputCursor.nextAsync();
        stages.add( recordResponse );

        recordResponse.whenComplete( ( record, error ) -> {
            if ( error != null )
            {
                resultFuture.completeExceptionally( error );
            }
            else if ( record != null )
            {
                runNestedQuery( inputCursor, record, stages, resultFuture );
            }
            else
            {
                resultFuture.complete( stages );
            }
        } );
    }

    private void runNestedQuery( StatementResultCursor inputCursor, Record record, List<CompletionStage<Record>> stages,
            CompletableFuture<List<CompletionStage<Record>>> resultFuture )
    {
        Node node = record.get( 0 ).asNode();
        long id = node.get( "id" ).asLong();
        long age = id * 10;

        CompletionStage<StatementResultCursor> response =
                session.runAsync( "MATCH (p:Person {id: $id}) SET p.age = $age RETURN p", parameters( "id", id, "age", age ) );

        response.whenComplete( ( cursor, error ) -> {
            if ( error != null )
            {
                resultFuture.completeExceptionally( Futures.completionExceptionCause( error ) );
            }
            else
            {
                stages.add( cursor.nextAsync() );
                runNestedQueries( inputCursor, stages, resultFuture );
            }
        } );
    }

    private long countNodesByLabel( String label )
    {
        CompletionStage<Long> countStage =
                session.runAsync( "MATCH (n:" + label + ") RETURN count(n)" ).thenCompose( StatementResultCursor::singleAsync ).thenApply(
                        record -> record.get( 0 ).asLong() );

        return await( countStage );
    }
}
