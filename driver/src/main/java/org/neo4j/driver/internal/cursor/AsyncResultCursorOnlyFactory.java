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
package org.neo4j.driver.internal.cursor;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.async.AsyncStatementResultCursor;
import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.exceptions.ClientException;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.messaging.request.PullAllMessage.PULL_ALL;

/**
 * Used by Bolt V1, V2, V3
 */
public class AsyncResultCursorOnlyFactory implements StatementResultCursorFactory
{
    protected final Connection connection;
    protected final Message runMessage;
    protected final RunResponseHandler runHandler;
    protected final PullAllResponseHandler pullAllHandler;
    private final boolean waitForRunResponse;

    public AsyncResultCursorOnlyFactory( Connection connection, Message runMessage, RunResponseHandler runHandler,
            PullAllResponseHandler pullHandler, boolean waitForRunResponse )
    {
        requireNonNull( connection );
        requireNonNull( runMessage );
        requireNonNull( runHandler );
        requireNonNull( pullHandler );

        this.connection = connection;
        this.runMessage = runMessage;
        this.runHandler = runHandler;

        this.pullAllHandler = pullHandler;
        this.waitForRunResponse = waitForRunResponse;
    }

    public CompletionStage<InternalStatementResultCursor> asyncResult()
    {
        // only write and flush messages when async result is wanted.
        connection.writeAndFlush( runMessage, runHandler, PULL_ALL, pullAllHandler );

        if ( waitForRunResponse )
        {
            // wait for response of RUN before proceeding
            return runHandler.runFuture().thenApply( ignore -> new AsyncStatementResultCursor( runHandler, pullAllHandler ) );
        }
        else
        {
            return completedFuture( new AsyncStatementResultCursor( runHandler, pullAllHandler ) );
        }
    }

    public CompletionStage<RxStatementResultCursor> rxResult()
    {
        return Futures.failedFuture( new ClientException( "Driver is connected to the database that does not support driver reactive API. " +
                "In order to use the driver reactive API, please upgrade to neo4j 4.0.0 or later." ) );
    }
}
