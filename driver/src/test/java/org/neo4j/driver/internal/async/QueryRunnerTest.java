/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.InternalStatementResultCursor;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.SessionPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.TransactionPullAllResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.async.QueryRunner.runInSession;
import static org.neo4j.driver.internal.async.QueryRunner.runInTransaction;
import static org.neo4j.driver.v1.Values.value;

class QueryRunnerTest
{
    private static final String QUERY = "RETURN $x";
    private static final Map<String,Value> PARAMS = singletonMap( "x", value( 42 ) );
    private static final Statement STATEMENT = new Statement( QUERY, value( PARAMS ) );

    @Test
    void shouldRunInSessionWithoutWaitingForRunResponse() throws Exception
    {
        testNotWaitingForRunResponse( true );
    }

    @Test
    void shouldRunInSessionAndWaitForSuccessRunResponse() throws Exception
    {
        testWaitingForRunResponse( true, true );
    }

    @Test
    void shouldRunInSessionAndWaitForFailureRunResponse() throws Exception
    {
        testWaitingForRunResponse( false, true );
    }

    @Test
    void shouldRunInTransactionWithoutWaitingForRunResponse() throws Exception
    {
        testNotWaitingForRunResponse( false );
    }

    @Test
    void shouldRunInTransactionAndWaitForSuccessRunResponse() throws Exception
    {
        testWaitingForRunResponse( true, false );
    }

    @Test
    void shouldRunInTransactionAndWaitForFailureRunResponse() throws Exception
    {
        testWaitingForRunResponse( false, false );
    }

    private static void testNotWaitingForRunResponse( boolean session ) throws Exception
    {
        Connection connection = mock( Connection.class );

        CompletionStage<InternalStatementResultCursor> cursorStage;
        if ( session )
        {

            cursorStage = runInSession( connection, STATEMENT, false );
        }
        else
        {
            cursorStage = runInTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), false );
        }
        CompletableFuture<InternalStatementResultCursor> cursorFuture = cursorStage.toCompletableFuture();

        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
        verifyRunInvoked( connection, session );
    }

    private static void testWaitingForRunResponse( boolean success, boolean session ) throws Exception
    {
        Connection connection = mock( Connection.class );

        CompletionStage<InternalStatementResultCursor> cursorStage;
        if ( session )
        {
            cursorStage = runInSession( connection, STATEMENT, true );
        }
        else
        {
            cursorStage = runInTransaction( connection, STATEMENT, mock( ExplicitTransaction.class ), true );
        }
        CompletableFuture<InternalStatementResultCursor> cursorFuture = cursorStage.toCompletableFuture();

        assertFalse( cursorFuture.isDone() );
        ResponseHandler runResponseHandler = verifyRunInvoked( connection, session );

        if ( success )
        {
            runResponseHandler.onSuccess( emptyMap() );
        }
        else
        {
            runResponseHandler.onFailure( new RuntimeException() );
        }

        assertTrue( cursorFuture.isDone() );
        assertNotNull( cursorFuture.get() );
    }

    private static ResponseHandler verifyRunInvoked( Connection connection, boolean session )
    {
        ArgumentCaptor<ResponseHandler> runHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );
        ArgumentCaptor<ResponseHandler> pullAllHandlerCaptor = ArgumentCaptor.forClass( ResponseHandler.class );

        verify( connection ).runAndFlush( eq( QUERY ), eq( PARAMS ),
                runHandlerCaptor.capture(), pullAllHandlerCaptor.capture() );

        assertThat( runHandlerCaptor.getValue(), instanceOf( RunResponseHandler.class ) );

        if ( session )
        {
            assertThat( pullAllHandlerCaptor.getValue(), instanceOf( SessionPullAllResponseHandler.class ) );
        }
        else
        {
            assertThat( pullAllHandlerCaptor.getValue(), instanceOf( TransactionPullAllResponseHandler.class ) );
        }

        return runHandlerCaptor.getValue();
    }
}
