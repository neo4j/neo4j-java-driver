/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.async;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.InternalStatementResultCursor;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.SessionPullAllResponseHandler;
import org.neo4j.driver.internal.handlers.TransactionPullAllResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.v1.Values.ofValue;

/**
 * Helper to execute queries in {@link Session} and {@link Transaction}. Query execution consists of sending
 * RUN and PULL_ALL messages. Different handles are used to process responses for those messages, depending on if
 * they were executed in session or transaction.
 */
public final class QueryRunner
{
    private QueryRunner()
    {
    }

    /**
     * Execute given statement for {@link Session#run(Statement)}.
     *
     * @param connection the network connection to use.
     * @param statement the cypher to execute.
     * @param waitForRunResponse {@code true} for async query execution and {@code false} for blocking query
     * execution. Makes returned cursor stage be chained after the RUN response arrives. Needed to have statement
     * keys populated.
     * @return stage with cursor.
     */
    public static CompletionStage<InternalStatementResultCursor> runInSession( Connection connection,
            Statement statement, boolean waitForRunResponse )
    {
        return run( connection, statement, null, waitForRunResponse );
    }

    /**
     * Execute given statement for {@link Transaction#run(Statement)}.
     *
     * @param connection the network connection to use.
     * @param statement the cypher to execute.
     * @param tx the transaction which executes the query.
     * @param waitForRunResponse {@code true} for async query execution and {@code false} for blocking query
     * execution. Makes returned cursor stage be chained after the RUN response arrives. Needed to have statement
     * keys populated.
     * @return stage with cursor.
     */
    public static CompletionStage<InternalStatementResultCursor> runInTransaction( Connection connection,
            Statement statement, ExplicitTransaction tx, boolean waitForRunResponse )
    {
        return run( connection, statement, tx, waitForRunResponse );
    }

    private static CompletionStage<InternalStatementResultCursor> run( Connection connection,
            Statement statement, ExplicitTransaction tx, boolean waitForRunResponse )
    {
        String query = statement.text();
        Map<String,Value> params = statement.parameters().asMap( ofValue() );

        CompletableFuture<Void> runCompletedFuture = new CompletableFuture<>();
        RunResponseHandler runHandler = new RunResponseHandler( runCompletedFuture );
        PullAllResponseHandler pullAllHandler = newPullAllHandler( statement, runHandler, connection, tx );

        connection.runAndFlush( query, params, runHandler, pullAllHandler );

        if ( waitForRunResponse )
        {
            // wait for response of RUN before proceeding
            return runCompletedFuture.thenApply( ignore ->
                    InternalStatementResultCursor.forAsyncRun( runHandler, pullAllHandler ) );
        }
        else
        {
            return completedFuture( InternalStatementResultCursor.forBlockingRun( runHandler, pullAllHandler ) );
        }
    }

    private static PullAllResponseHandler newPullAllHandler( Statement statement, RunResponseHandler runHandler,
            Connection connection, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullAllResponseHandler( statement, runHandler, connection, tx );
        }
        return new SessionPullAllResponseHandler( statement, runHandler, connection );
    }
}
