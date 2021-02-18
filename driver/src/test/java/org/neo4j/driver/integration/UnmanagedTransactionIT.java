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
import org.neo4j.driver.Query;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.InternalDriver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlanImpl;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.io.ChannelTrackingDriverFactory;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.util.TestUtil.await;


@ParallelizableIT
class UnmanagedTransactionIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private NetworkSession session;

    @BeforeEach
    void setUp()
    {
        session = ((InternalDriver) neo4j.driver()).newSession( SessionConfig.defaultConfig() );
    }

    @AfterEach
    void tearDown()
    {
        session.closeAsync();
    }

    private UnmanagedTransaction beginTransaction()
    {
        return beginTransaction( session );
    }

    private UnmanagedTransaction beginTransaction(NetworkSession session )
    {
        return await( session.beginTransactionAsync( TransactionConfig.empty() ) );
    }

    private ResultCursor sessionRun(NetworkSession session, Query query)
    {
        return await( session.runAsync(query, TransactionConfig.empty(), true ) );
    }

    private ResultCursor txRun(UnmanagedTransaction tx, String query )
    {
        return await( tx.runAsync( new Query( query ), true ) );
    }

    @Test
    void shouldDoNothingWhenCommittedSecondTime()
    {
        UnmanagedTransaction tx = beginTransaction();

        assertNull( await( tx.commitAsync() ) );

        assertTrue( tx.commitAsync().toCompletableFuture().isDone() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldFailToCommitAfterRollback()
    {
        UnmanagedTransaction tx = beginTransaction();

        assertNull( await( tx.rollbackAsync() ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertEquals( "Can't commit, transaction has been rolled back", e.getMessage() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldFailToCommitAfterTermination()
    {
        UnmanagedTransaction tx = beginTransaction();

        tx.markTerminated( null );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );
        assertThat( e.getMessage(), startsWith( "Transaction can't be committed" ) );
    }

    @Test
    void shouldDoNothingWhenRolledBackSecondTime()
    {
        UnmanagedTransaction tx = beginTransaction();

        assertNull( await( tx.rollbackAsync() ) );

        assertTrue( tx.rollbackAsync().toCompletableFuture().isDone() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldFailToRollbackAfterCommit()
    {
        UnmanagedTransaction tx = beginTransaction();

        assertNull( await( tx.commitAsync() ) );

        ClientException e = assertThrows( ClientException.class, () -> await( tx.rollbackAsync() ) );
        assertEquals( "Can't rollback, transaction has been committed", e.getMessage() );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldRollbackAfterTermination()
    {
        UnmanagedTransaction tx = beginTransaction();

        tx.markTerminated( null );

        assertNull( await( tx.rollbackAsync() ) );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldFailToRunQueryWhenTerminated()
    {
        UnmanagedTransaction tx = beginTransaction();
        txRun( tx, "CREATE (:MyLabel)" );
        tx.markTerminated( null );

        ClientException e = assertThrows( ClientException.class, () -> txRun( tx, "CREATE (:MyOtherLabel)" ) );
        assertThat( e.getMessage(), startsWith( "Cannot run more queries in this transaction" ) );
    }

    @Test
    void shouldBePossibleToRunMoreTransactionsAfterOneIsTerminated()
    {
        UnmanagedTransaction tx1 = beginTransaction();
        tx1.markTerminated( null );

        // commit should fail, make session forget about this transaction and release the connection to the pool
        ClientException e = assertThrows( ClientException.class, () -> await( tx1.commitAsync() ) );
        assertThat( e.getMessage(), startsWith( "Transaction can't be committed" ) );

        await( session.beginTransactionAsync( TransactionConfig.empty() )
                .thenCompose( tx -> tx.runAsync( new Query( "CREATE (:Node {id: 42})" ), true )
                        .thenCompose( ResultCursor::consumeAsync )
                        .thenApply( ignore -> tx )
                ).thenCompose( UnmanagedTransaction::commitAsync ) );

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
        Query query = new Query( "MATCH (n:Node {id: $id}) RETURN count(n)", parameters( "id", id ) );
        ResultCursor cursor = sessionRun( session, query);
        return await( cursor.singleAsync() ).get( 0 ).asInt();
    }

    private void testCommitAndRollbackFailurePropagation( boolean commit )
    {
        ChannelTrackingDriverFactory driverFactory = new ChannelTrackingDriverFactory( 1, Clock.SYSTEM );
        Config config = Config.builder().withLogging( DEV_NULL_LOGGING ).build();

        try ( Driver driver = driverFactory.newInstance( neo4j.uri(), neo4j.authToken(), RoutingSettings.DEFAULT, RetrySettings.DEFAULT, config,
                                                         SecurityPlanImpl.insecure() ) )
        {
            NetworkSession session = ((InternalDriver) driver).newSession( SessionConfig.defaultConfig() );
            {
                UnmanagedTransaction tx = beginTransaction( session );

                // run query but do not consume the result
                txRun( tx, "UNWIND range(0, 10000) AS x RETURN x + 1" );

                IOException ioError = new IOException( "Connection reset by peer" );
                for ( Channel channel : driverFactory.channels() )
                {
                    // make channel experience a fatal network error
                    // run in the event loop thread and wait for the whole operation to complete
                    Future<ChannelPipeline> future = channel.eventLoop().submit( () -> channel.pipeline().fireExceptionCaught( ioError ) );
                    await( future );
                }

                CompletionStage<Void> commitOrRollback = commit ? tx.commitAsync() : tx.rollbackAsync();

                // commit/rollback should fail and propagate the network error
                ServiceUnavailableException e = assertThrows( ServiceUnavailableException.class, () -> await( commitOrRollback ) );
                assertEquals( ioError, e.getCause() );
            }
        }
    }
}
