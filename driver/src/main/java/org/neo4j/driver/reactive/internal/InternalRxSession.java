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
package org.neo4j.driver.reactive.internal;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.driver.reactive.RxTransactionWork;
import org.neo4j.driver.reactive.internal.cursor.RxStatementResultCursor;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.exceptions.TransientException;

import static org.neo4j.driver.reactive.internal.RxUtils.createEmptyPublisher;
import static org.neo4j.driver.reactive.internal.RxUtils.createMono;

public class InternalRxSession extends AbstractRxStatementRunner implements RxSession
{
    private final NetworkSession session;

    public InternalRxSession( NetworkSession session )
    {
        // RxSession accept a network session as input.
        // The network session different from async session that it provides ways to both run for Rx and Async
        // Note: Blocking result could just build on top of async result. However Rx result cannot just build on top of async result.
        this.session = session;
    }

    @Override
    public Publisher<RxTransaction> beginTransaction()
    {
        return beginTransaction( TransactionConfig.empty() );
    }

    @Override
    public Publisher<RxTransaction> beginTransaction( TransactionConfig config )
    {
        return createMono( () -> {
            CompletionStage<Transaction> txFuture = session.beginTransactionAsync();
            return txFuture.thenApply( transaction -> new InternalRxTransaction( (ExplicitTransaction) transaction ) );
        } );
    }

    @Override
    public <T> Publisher<T> readTransaction( RxTransactionWork<Publisher<T>> work )
    {
        return readTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> Publisher<T> readTransaction( RxTransactionWork<Publisher<T>> work, TransactionConfig config )
    {
        return runTransaction( AccessMode.READ, work, config );
    }

    @Override
    public <T> Publisher<T> writeTransaction( RxTransactionWork<Publisher<T>> work )
    {
        return writeTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> Publisher<T> writeTransaction( RxTransactionWork<Publisher<T>> work, TransactionConfig config )
    {
        return runTransaction( AccessMode.WRITE, work, config );
    }

    private <T> Publisher<T> runTransaction( AccessMode mode, RxTransactionWork<Publisher<T>> work, TransactionConfig config )
    {
        // TODO read and write
        Publisher<RxTransaction> publisher = beginTransaction( config );
        Flux<T> txExecutor = Mono.from( publisher ).flatMapMany( tx -> Flux.from( work.execute( tx ) ).flatMap( t -> Mono.create( sink -> sink.success( t ) ),
                throwable -> Mono.from( tx.rollback() ).then( Mono.error( throwable ) ), // TODO chain errors from rollback to throwable
                () -> Mono.from( tx.commit() ).then( Mono.empty() ) ) );
        return txExecutor.retry( throwable -> {
            if ( throwable instanceof TransientException )
            {
                return true;
            }
            else
            {
                return false;
            }
        } ); // TODO retry
    }

    @Override
    public RxResult run( String statement, TransactionConfig config )
    {
        return run( new Statement( statement ), config );
    }

    @Override
    public RxResult run( String statement, Map<String,Object> parameters, TransactionConfig config )
    {
        return run( new Statement( statement, parameters ), config );
    }

    @Override
    public RxResult run( Statement statement )
    {
        return run( statement, TransactionConfig.empty() );
    }

    @Override
    public RxResult run( Statement statement, TransactionConfig config )
    {
        return new InternalRxResult( () -> {
            CompletableFuture<RxStatementResultCursor> resultCursorFuture = new CompletableFuture<>();
            session.runRx( statement, config ).whenComplete( ( cursor, completionError ) -> {
                if ( cursor != null )
                {
                    resultCursorFuture.complete( cursor );
                }
                else
                {
                    // We failed to create a result cursor so we cannot rely on result cursor to cleanup resources.
                    // Therefore we will first release the connection that might have been created in the session and then notify the error.
                    // The logic here shall be the same as `SessionPullResponseHandler#afterFailure`.
                    // The reason we need to release connection in session is that we do not have a `rxSession.close()`;
                    // Otherwise, session.close shall handle everything for us.
                    Throwable error = Futures.completionExceptionCause( completionError );
                    session.releaseConnection().whenComplete( ( ignored, closeError ) ->
                            resultCursorFuture.completeExceptionally( Futures.combineErrors( error, closeError ) ) );
                }
            } );
            return resultCursorFuture;
        } );
    }

    @Override
    public String lastBookmark()
    {
        return session.lastBookmark();
    }

    public Publisher<Void> reset()
    {
        return createEmptyPublisher( session::resetAsync );
    }

}
