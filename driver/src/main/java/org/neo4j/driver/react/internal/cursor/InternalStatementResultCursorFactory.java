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
package org.neo4j.driver.react.internal.cursor;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.AsyncPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;

import static java.util.Objects.requireNonNull;

public class InternalStatementResultCursorFactory implements StatementResultCursorFactory
{
    private final RunResponseHandler runHandler;
    private final Connection connection;

    private final BasicPullResponseHandler pullHandler;
    private final boolean waitForRunResponse;
    private final Message runMessage;

    public InternalStatementResultCursorFactory( Connection connection, Message runMessage, RunResponseHandler runHandler, BasicPullResponseHandler pullHandler,
            boolean waitForRunResponse )
    {
        requireNonNull( connection );
        requireNonNull( runMessage );
        requireNonNull( runHandler );
        requireNonNull( pullHandler );

        this.connection = connection;
        this.runMessage = runMessage;
        this.runHandler = runHandler;
        this.pullHandler = pullHandler;
        this.waitForRunResponse = waitForRunResponse;
    }

    @Override
    public CompletionStage<InternalStatementResultCursor> asyncResult()
    {
        connection.writeAndFlush( runMessage, runHandler );
        if ( waitForRunResponse )
        {
            return runHandler.runFuture().thenApply( ignored -> composeAsyncCursor() );
        }
        else
        {
            return Futures.completedWithValue( composeAsyncCursor() );
        }
    }

    @Override
    public CompletionStage<RxStatementResultCursor> rxResult()
    {
        connection.writeAndFlush( runMessage, runHandler );
        if ( waitForRunResponse )
        {
            return runHandler.runFuture().thenApply( ignored -> composeRxCursor() );
        }
        else
        {
            return Futures.completedWithValue( composeRxCursor() );
        }
    }

    private AsyncStatementResultCursor composeAsyncCursor()
    {
        return new AsyncStatementResultCursor( runHandler, new AsyncPullResponseHandler( pullHandler ) );
    }

    private RxStatementResultCursor composeRxCursor()
    {
        return new RxStatementResultCursor( runHandler, pullHandler );
    }
}
