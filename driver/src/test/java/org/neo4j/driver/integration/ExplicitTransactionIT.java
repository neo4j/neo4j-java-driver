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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.async.EventLoopGroupFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactory;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Matchers.blockingOperationInEventLoopError;
import static org.neo4j.driver.util.TestUtil.await;

/**
 * We leave the question whether we want to let {@link ExplicitTransaction} both implements blocking tx and async tx or not later.
 * But as how it is right not, here are some tests for using mixes blocking and async API.
 */

@ParallelizableIT
class ExplicitTransactionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private AsyncSession session;

    @BeforeEach
    void setUp()
    {
        session = neo4j.driver().asyncSession();
    }

    @AfterEach
    void tearDown()
    {
        session.closeAsync();
    }

    @Test
    void shouldDoNothingWhenCommittedSecondTime()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        assertNull( await( tx.commitAsync() ) );

        assertTrue( tx.commitAsync().toCompletableFuture().isDone() );
        assertFalse( ((ExplicitTransaction) tx).isOpen() );
    }

    @Test
    void shouldFailToCommitAfterRollback()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        assertNull( await( tx.rollbackAsync() ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertEquals( "Can't commit, transaction has been rolled back", e.getMessage() );
        assertFalse( ((ExplicitTransaction) tx).isOpen() );
    }

    @Test
    void shouldFailToCommitAfterTermination()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        ((ExplicitTransaction) tx).markTerminated();

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.getMessage(), startsWith( "Transaction can't be committed" ) );
    }

    @Test
    void shouldDoNothingWhenRolledBackSecondTime()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        assertNull( await( tx.rollbackAsync() ) );

        assertTrue( tx.rollbackAsync().toCompletableFuture().isDone() );
        assertFalse( ((ExplicitTransaction) tx).isOpen() );
    }

    @Test
    void shouldFailToRollbackAfterCommit()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        assertNull( await( tx.commitAsync() ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertEquals( "Can't rollback, transaction has been committed", e.getMessage() );
        assertFalse( ((ExplicitTransaction) tx).isOpen() );
    }

    @Test
    void shouldRollbackAfterTermination()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );

        ((ExplicitTransaction) tx).markTerminated();

        assertNull( await( tx.rollbackAsync() ) );
        assertFalse( ((ExplicitTransaction) tx).isOpen() );
    }

    @Test
    void shouldFailToRunQueryWhenMarkedForFailure()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        ((ExplicitTransaction) tx).failure();

        ClientException e = assertThrows( ClientException.class, () -> await( tx.runAsync( "CREATE (:MyOtherLabel)" ) ) );
        assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );
    }

    @Test
    void shouldFailToRunQueryWhenTerminated()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );
        ((ExplicitTransaction) tx).markTerminated();

        ClientException e = assertThrows( ClientException.class, () -> await( tx.runAsync( "CREATE (:MyOtherLabel)" ) ) );
        assertThat( e.getMessage(), startsWith( "Cannot run more statements in this transaction" ) );
    }

    @Test
    void shouldAllowQueriesWhenMarkedForSuccess()
    {
        AsyncTransaction tx = await( session.beginTransactionAsync() );
        tx.runAsync( "CREATE (:MyLabel)" );

        ((ExplicitTransaction) tx).success();

        tx.runAsync( "CREATE (:MyLabel)" );
        assertNull( await( tx.commitAsync() ) );

        StatementResultCursor cursor = await( session.runAsync( "MATCH (n:MyLabel) RETURN count(n)" ) );
        assertEquals( 2, await( cursor.singleAsync() ).get( 0 ).asInt() );
    }

    @Test
    void shouldFailToExecuteBlockingRunChainedWithAsyncTransaction()
    {
        CompletionStage<Void> result = session.beginTransactionAsync()
                .thenApply( tx ->
                {
                    if ( EventLoopGroupFactory.isEventLoopThread( Thread.currentThread() ) )
                    {
                        IllegalStateException e = assertThrows( IllegalStateException.class, () -> ((ExplicitTransaction) tx).run( "CREATE ()" ) );
                        assertThat( e, is( blockingOperationInEventLoopError() ) );
                    }
                    return null;
                } );

        assertNull( await( result ) );
    }

    @Test
    void shouldAllowUsingBlockingApiInCommonPoolWhenChaining()
    {
        CompletionStage<AsyncTransaction> txStage = session.beginTransactionAsync()
                // move execution to ForkJoinPool.commonPool()
                .thenApplyAsync( asyncTx ->
                {
                    Transaction tx = (ExplicitTransaction) asyncTx;
                    tx.run( "UNWIND [1,1,2] AS x CREATE (:Node {id: x})" );
                    tx.run( "CREATE (:Node {id: 42})" );
                    tx.success();
                    tx.close();
                    return asyncTx;
                } );

        AsyncTransaction tx = await( txStage );

        assertFalse( ((ExplicitTransaction) tx).isOpen() );
        assertEquals( 2, countNodes( 1 ) );
        assertEquals( 1, countNodes( 2 ) );
        assertEquals( 1, countNodes( 42 ) );
    }

    @Test
    void shouldBePossibleToRunMoreTransactionsAfterOneIsTerminated()
    {
        AsyncTransaction tx1 = await( session.beginTransactionAsync() );
        ((ExplicitTransaction) tx1).markTerminated();

        // commit should fail, make session forget about this transaction and release the connection to the pool
        ClientException e = assertThrows( ClientException.class, () -> await( tx1.commitAsync() ) );
        assertThat( e.getMessage(), startsWith( "Transaction can't be committed" ) );

        await( session.beginTransactionAsync()
                .thenCompose( tx -> tx.runAsync( "CREATE (:Node {id: 42})" )
                        .thenCompose( StatementResultCursor::consumeAsync )
                        .thenApply( ignore -> tx )
                ).thenCompose( AsyncTransaction::commitAsync ) );

        assertEquals( 1, countNodes( 42 ) );
    }

    @Test
    void shouldPropagateCommitFailureAfterFatalError()
    {
        testCommitAndRollbackFailurePropagation( true );
    }

    @Test
    void shouldPropagateRollbackFailureAfterFatalError()
    {
        testCommitAndRollbackFailurePropagation( false );
    }

    private int countNodes( Object id )
    {
        StatementResultCursor cursor = await( session.runAsync( "MATCH (n:Node {id: $id}) RETURN count(n)", parameters( "id", id ) ) );
        return await( cursor.singleAsync() ).get( 0 ).asInt();
    }

    private void testCommitAndRollbackFailurePropagation( boolean commit )
    {
        ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );
        Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).build();

        try ( Driver driver = driverFactory.newInstance( neo4j.uri(), neo4j.authToken(), RoutingSettings.DEFAULT, RetrySettings.DEFAULT, config ) )
        {
            try ( Session session = driver.session() )
            {
                Transaction tx = session.beginTransaction();

                // run query but do not consume the result
                tx.run( "UNWIND range(0, 10000) AS x RETURN x + 1" );

                IOException ioError = new IOException( "Connection reset by peer" );
                for ( Channel channel : driverFactory.channels() )
                {
                    // make channel experience a fatal network error
                    // run in the event loop thread and wait for the whole operation to complete
                    Future<ChannelPipeline> future = channel.eventLoop().submit( () -> channel.pipeline().fireExceptionCaught( ioError ) );
                    await( future );
                }

                AsyncTransaction asyncTx = (ExplicitTransaction) tx;
                CompletionStage<Void> commitOrRollback = commit ? asyncTx.commitAsync() : asyncTx.rollbackAsync();

                // commit/rollback should fail and propagate the network error
                ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> await( commitOrRollback ) );
                assertEquals( ioError, e.getCause() );
            }
        }
    }
}
