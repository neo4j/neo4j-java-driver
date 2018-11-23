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
package org.neo4j.driver.react;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.react.result.RxStatementResultCursor;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.exceptions.TransientException;

public class InternalRxSession extends AbstractRxStatementRunner implements RxSession
{
    private final NetworkSession asyncSession;

    public InternalRxSession( Session asyncSession )
    {
        this.asyncSession = (NetworkSession) asyncSession;
    }

    @Override
    public Publisher<RxTransaction> beginTransaction()
    {
        return beginTransaction( TransactionConfig.empty() );
    }

    @Override
    public Publisher<RxTransaction> beginTransaction( TransactionConfig config )
    {
        return Mono.create( sink -> {
            CompletionStage<Transaction> txFuture = asyncSession.beginTransactionAsync();
            CompletionStage<RxTransaction> rxTxFuture = txFuture.thenApply( transaction -> new InternalRxTransaction( (ExplicitTransaction) transaction ) );
            rxTxFuture.whenComplete( ( tx, error ) -> {
                if ( tx != null )
                {
                    sink.success( tx );
                }
                else if ( error != null )
                {
                    sink.error( error );
                }
                else
                {
                    sink.success();
                }
            } );
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
                System.out.println( throwable );
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
        CompletionStage<RxStatementResultCursor> cursor = asyncSession.runRx( statement, config );
        return new InternalRxResult( cursor );
    }

    @Override
    public String lastBookmark()
    {
        return null;
    }

    @Override
    public void reset()
    {
    }

    @Override
    public Publisher<Void> close()
    {
        return Mono.create( sink -> {
            asyncSession.closeAsync().whenComplete( ( ignore, error ) -> {
                if ( error != null )
                {
                    sink.error( error );
                }
                else
                {
                    sink.success();
                }
            } );
        } );
    }
}
