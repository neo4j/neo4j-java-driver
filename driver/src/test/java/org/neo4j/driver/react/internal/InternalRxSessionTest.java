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
package org.neo4j.driver.react.internal;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.react.RxResult;
import org.neo4j.driver.react.internal.cursor.RxStatementResultCursor;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.TransactionConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InternalRxSessionTest
{
    @Test
    void shouldDelegateRun() throws Throwable
    {
        // Given
        NetworkSession session = mock( NetworkSession.class );
        RxStatementResultCursor cursor = mock( RxStatementResultCursor.class );

        // Run succeeded with a cursor
        when( session.runRx( any( Statement.class ), any( TransactionConfig.class ) ) ).thenReturn( Futures.completedWithValue( cursor ) );
        InternalRxSession rxSession = new InternalRxSession( session );

        // When
        RxResult result = rxSession.run( "RETURN 1" );
        // Execute the run
        CompletionStage<RxStatementResultCursor> cursorFuture = ((InternalRxResult) result).cursorFutureSupplier().get();

        // Then
        verify( session ).runRx( any( Statement.class ), any( TransactionConfig.class ) );
        assertThat( Futures.getNow( cursorFuture ), equalTo( cursor ) );
    }

    @Test
    void shouldReleaseConnectionIfFailedToRun() throws Throwable
    {
        // Given
        Throwable error = new RuntimeException( "Hi there" );
        NetworkSession session = mock( NetworkSession.class );

        // Run failed with error
        when( session.runRx( any( Statement.class ), any( TransactionConfig.class ) ) ).thenReturn( Futures.failedFuture( error ) );
        when( session.releaseConnection() ).thenReturn( Futures.completedWithNull() );

        InternalRxSession rxSession = new InternalRxSession( session );

        // When
        RxResult result = rxSession.run( "RETURN 1" );
        // Execute the run
        CompletionStage<RxStatementResultCursor> cursorFuture = ((InternalRxResult) result).cursorFutureSupplier().get();

        // Then
        verify( session ).runRx( any( Statement.class ), any( TransactionConfig.class ) );
        RuntimeException t = assertThrows( CompletionException.class, () -> Futures.getNow( cursorFuture ) );
        assertThat( t.getCause(), equalTo( error ) );
    }
}
