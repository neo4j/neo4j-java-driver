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
package org.neo4j.driver.internal.reactive;

import org.neo4j.driver.Query;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxTransaction;

import static org.neo4j.driver.internal.reactive.RxUtils.createEmptyPublisher;

public class InternalRxTransaction extends AbstractRxQueryRunner implements RxTransaction
{
    private final UnmanagedTransaction tx;

    public InternalRxTransaction( UnmanagedTransaction tx )
    {
        this.tx = tx;
    }

    @Override
    public RxResult run(Query query)
    {
        return new InternalRxResult( () -> {
            CompletableFuture<RxResultCursor> cursorFuture = new CompletableFuture<>();
            tx.runRx(query).whenComplete( (cursor, completionError ) -> {
                if ( cursor != null )
                {
                    cursorFuture.complete( cursor );
                }
                else
                {
                    // We failed to create a result cursor so we cannot rely on result cursor to handle failure.
                    // The logic here shall be the same as `TransactionPullResponseHandler#afterFailure` as that is where cursor handling failure
                    // This is optional as tx still holds a reference to all cursor futures and they will be clean up properly in commit
                    Throwable error = Futures.completionExceptionCause( completionError );
                    tx.markTerminated( error );
                    cursorFuture.completeExceptionally( error );
                }
            } );
            return cursorFuture;
        } );
    }

    @Override
    public <T> Publisher<T> commit()
    {
        return close( true );
    }

    @Override
    public <T> Publisher<T> rollback()
    {
        return close( false );
    }

    private <T> Publisher<T> close( boolean commit )
    {
        return createEmptyPublisher( () -> {
            if ( commit )
            {
                return tx.commitAsync();
            }
            else
            {
                return tx.rollbackAsync();
            }
        } );
    }
}
