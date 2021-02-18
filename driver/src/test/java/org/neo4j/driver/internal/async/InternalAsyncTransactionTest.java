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
package org.neo4j.driver.internal.async;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.util.TestUtil.await;
import static org.neo4j.driver.util.TestUtil.connectionMock;
import static org.neo4j.driver.util.TestUtil.newSession;
import static org.neo4j.driver.util.TestUtil.setupFailingCommit;
import static org.neo4j.driver.util.TestUtil.setupFailingRollback;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.util.TestUtil.verifyCommitTx;
import static org.neo4j.driver.util.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.util.TestUtil.verifyRunAndPull;

class InternalAsyncTransactionTest
{
    private Connection connection;
    private NetworkSession networkSession;
    private InternalAsyncTransaction tx;

    @BeforeEach
    void setUp()
    {
        connection = connectionMock( BoltProtocolV4.INSTANCE );
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        when( connectionProvider.acquireConnection( any( ConnectionContext.class ) ) )
                .thenReturn( completedFuture( connection ) );
        networkSession = newSession(connectionProvider);
        InternalAsyncSession session = new InternalAsyncSession(networkSession);
        tx = (InternalAsyncTransaction) await( session.beginTransactionAsync() );
    }

    private static Stream<Function<AsyncTransaction,CompletionStage<ResultCursor>>> allSessionRunMethods()
    {
        return Stream.of(
                tx -> tx.runAsync( "RETURN 1" ),
                tx -> tx.runAsync( "RETURN $x", parameters( "x", 1 ) ),
                tx -> tx.runAsync( "RETURN $x", singletonMap( "x", 1 ) ),
                tx -> tx.runAsync( "RETURN $x",
                        new InternalRecord( singletonList( "x" ), new Value[]{new IntegerValue( 1 )} ) ),
                tx -> tx.runAsync( new Query( "RETURN $x", parameters( "x", 1 ) ) )
        );
    }

    @ParameterizedTest
    @MethodSource( "allSessionRunMethods" )
    void shouldFlushOnRun( Function<AsyncTransaction,CompletionStage<ResultCursor>> runReturnOne )
    {
        setupSuccessfulRunAndPull( connection );

        ResultCursor result = await( runReturnOne.apply( tx ) );
        ResultSummary summary = await( result.consumeAsync() );

        verifyRunAndPull( connection, summary.query().text() );
    }

    @Test
    void shouldCommit()
    {
        await( tx.commitAsync() );

        verifyCommitTx( connection );
        verify( connection ).release();
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldRollback()
    {
        await( tx.rollbackAsync() );

        verifyRollbackTx( connection );
        verify( connection ).release();
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldRollbackWhenFailedRun()
    {
        Futures.blockingGet( networkSession.resetAsync() );
        ClientException clientException = assertThrows( ClientException.class, () -> await( tx.commitAsync() ) );

        assertThat( clientException.getMessage(), containsString( "It has been rolled back either because of an error or explicit termination" ) );
        verify( connection ).release();
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldReleaseConnectionWhenFailedToCommit()
    {
        setupFailingCommit( connection );
        assertThrows( Exception.class, () -> await( tx.commitAsync() ) );

        verify( connection ).release();
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldReleaseConnectionWhenFailedToRollback()
    {
        setupFailingRollback( connection );
        assertThrows( Exception.class, () -> await( tx.rollbackAsync() ) );

        verify( connection ).release();
        assertFalse( tx.isOpen() );
    }
}
