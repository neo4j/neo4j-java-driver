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

import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.react.RxResult;
import org.neo4j.driver.react.RxTransaction;
import org.neo4j.driver.react.internal.cursor.RxStatementResultCursor;
import org.neo4j.driver.v1.Statement;

import static org.neo4j.driver.react.internal.RxUtils.createEmptyPublisher;

public class InternalRxTransaction extends AbstractRxStatementRunner implements RxTransaction
{
    private final ExplicitTransaction asyncTx;

    public InternalRxTransaction( ExplicitTransaction asyncTx )
    {
        this.asyncTx = asyncTx;
    }

    @Override
    public Publisher<Void> commit()
    {
        return close( true );
    }

    private Publisher<Void> close( boolean commit )
    {
        return createEmptyPublisher( () -> {
            if ( commit )
            {
                return asyncTx.commitAsync();
            }
            else
            {
                return asyncTx.rollbackAsync();
            }
        } );
    }

    @Override
    public Publisher<Void> rollback()
    {
        return close( false );
    }

    @Override
    public RxResult run( Statement statement )
    {
        return new InternalRxResult( () -> {
            CompletableFuture<RxStatementResultCursor> cursorFuture = new CompletableFuture<>();
            asyncTx.runRx( statement ).whenComplete( ( cursor, completionError ) -> {
                if ( cursor != null )
                {
                    cursorFuture.complete( cursor );
                }
                else
                {
                    // We failed to create a result cursor so we cannot rely on result cursor to handle failure.
                    // The logic here shall be the same as `TransactionPullResponseHandler#afterFailure` as that is where cursor handling failure
                    // This is optional as asyncTx still holds a reference to all cursor futures and they will be clean up properly in commit
                    Throwable error = Futures.completionExceptionCause( completionError );
                    asyncTx.markTerminated();
                    cursorFuture.completeExceptionally( error );
                }
            } );
            return cursorFuture;
        } );
    }
}
