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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.async.ConnectionContext;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.util.TestUtil.connectionMock;
import static org.neo4j.driver.util.TestUtil.newSession;
import static org.neo4j.driver.util.TestUtil.setupFailingCommit;
import static org.neo4j.driver.util.TestUtil.setupFailingRollback;
import static org.neo4j.driver.util.TestUtil.setupFailingRun;
import static org.neo4j.driver.util.TestUtil.setupSuccessfulRunAndPull;
import static org.neo4j.driver.util.TestUtil.verifyCommitTx;
import static org.neo4j.driver.util.TestUtil.verifyRollbackTx;
import static org.neo4j.driver.util.TestUtil.verifyRunAndPull;

class InternalTransactionTest
{
    private Connection connection;
    private Transaction tx;

    @BeforeEach
    void setUp()
    {
        connection = connectionMock( BoltProtocolV4.INSTANCE );
        ConnectionProvider connectionProvider = mock( ConnectionProvider.class );
        when( connectionProvider.acquireConnection( any( ConnectionContext.class ) ) )
                .thenReturn( completedFuture( connection ) );
        InternalSession session = new InternalSession( newSession( connectionProvider ) );
        tx = session.beginTransaction();
    }

    private static Stream<Function<Transaction, Result>> allSessionRunMethods()
    {
        return Stream.of(
                tx -> tx.run( "RETURN 1" ),
                tx -> tx.run( "RETURN $x", parameters( "x", 1 ) ),
                tx -> tx.run( "RETURN $x", singletonMap( "x", 1 ) ),
                tx -> tx.run( "RETURN $x",
                        new InternalRecord( singletonList( "x" ), new Value[]{new IntegerValue( 1 )} ) ),
                tx -> tx.run( new Query( "RETURN $x", parameters( "x", 1 ) ) )
        );
    }

    @ParameterizedTest
    @MethodSource( "allSessionRunMethods" )
    void shouldFlushOnRun( Function<Transaction, Result> runReturnOne ) throws Throwable
    {
        setupSuccessfulRunAndPull( connection );

        Result result = runReturnOne.apply( tx );
        ResultSummary summary = result.consume();

        verifyRunAndPull( connection, summary.query().text() );
    }

    @Test
    void shouldCommit() throws Throwable
    {
        tx.commit();
        tx.close();

        verifyCommitTx( connection );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldRollbackByDefault() throws Throwable
    {
        tx.close();

        verifyRollbackTx( connection );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldRollback() throws Throwable
    {
        tx.rollback();
        tx.close();

        verifyRollbackTx( connection );
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldRollbackWhenFailedRun() throws Throwable
    {
        setupFailingRun( connection, new RuntimeException( "Bang!" ) );
        assertThrows( RuntimeException.class, () -> tx.run( "RETURN 1" ).consume() );

        tx.close();

        verify( connection ).release();
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldReleaseConnectionWhenFailedToCommit() throws Throwable
    {
        setupFailingCommit( connection );
        assertThrows( Exception.class, () -> tx.commit() );

        verify( connection ).release();
        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldReleaseConnectionWhenFailedToRollback() throws Throwable
    {
        shouldReleaseConnectionWhenFailedToAction( Transaction::rollback );
    }

    @Test
    void shouldReleaseConnectionWhenFailedToClose() throws Throwable
    {
        shouldReleaseConnectionWhenFailedToAction( Transaction::close );
    }

    private void shouldReleaseConnectionWhenFailedToAction( Consumer<Transaction> txAction )
    {
        setupFailingRollback( connection );
        assertThrows( Exception.class, () -> txAction.accept( tx ) );

        verify( connection ).release();
        assertFalse( tx.isOpen() );
    }
}
