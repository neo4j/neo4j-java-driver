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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.neo4j.driver.Record;
import org.neo4j.driver.internal.cursor.RxResultCursor;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.summary.ResultSummary;

import static org.neo4j.driver.internal.util.ErrorUtil.newResultConsumedError;
import static reactor.core.publisher.FluxSink.OverflowStrategy.IGNORE;

public class InternalRxResult implements RxResult
{
    private Supplier<CompletionStage<RxResultCursor>> cursorFutureSupplier;
    private volatile CompletionStage<RxResultCursor> cursorFuture;

    public InternalRxResult(Supplier<CompletionStage<RxResultCursor>> cursorFuture )
    {
        this.cursorFutureSupplier = cursorFuture;
    }

    @Override
    public Publisher<List<String>> keys()
    {
        return Mono.defer( () -> Mono.fromCompletionStage( getCursorFuture() ).map( RxResultCursor::keys )
                .onErrorMap( Futures::completionExceptionCause ) );
    }

    @Override
    public Publisher<Record> records()
    {
        return Flux.create( sink -> getCursorFuture().whenComplete( ( cursor, completionError ) -> {
            if( cursor != null )
            {
                if( cursor.isDone() )
                {
                    sink.error( newResultConsumedError() );
                }
                else
                {
                    cursor.installRecordConsumer( createRecordConsumer( sink ) );
                    sink.onCancel( cursor::cancel );
                    sink.onRequest( cursor::request );
                }
            }
            else
            {
                Throwable error = Futures.completionExceptionCause( completionError );
                sink.error( error );
            }
        } ), IGNORE );
    }

    /**
     * Defines how a subscriber shall consume records.
     * A record consumer holds a reference to a subscriber.
     * A publisher and/or a subscription who holds a reference to this consumer shall release the reference to this object
     * after subscription is done or cancelled so that the subscriber can be garbage collected.
     * @param sink the subscriber
     * @return a record consumer.
     */
    private BiConsumer<Record,Throwable> createRecordConsumer( FluxSink<Record> sink )
    {
        return ( r, e ) -> {
            if ( r != null )
            {
                sink.next( r );
            }
            else if ( e != null )
            {
                sink.error( e );
            }
            else
            {
                sink.complete();
            }
        };
    }

    private CompletionStage<RxResultCursor> getCursorFuture()
    {
        if ( cursorFuture != null )
        {
            return cursorFuture;
        }
        return initCursorFuture();
    }

    synchronized CompletionStage<RxResultCursor> initCursorFuture()
    {
        // A quick path to return
        if ( cursorFuture != null )
        {
            return cursorFuture;
        }

        // now we obtained lock and we are going to be the one who assigns cursorFuture one and only once.
        cursorFuture = cursorFutureSupplier.get();
        cursorFutureSupplier = null; // we no longer need the reference to this object
        return this.cursorFuture;
    }

    @Override
    public Publisher<ResultSummary> consume()
    {
        return Mono.create( sink -> getCursorFuture().whenComplete( ( cursor, completionError ) -> {
            if ( cursor != null )
            {
                cursor.summaryAsync().whenComplete( ( summary, summaryCompletionError ) -> {
                    Throwable error = Futures.completionExceptionCause( summaryCompletionError );
                    if ( summary != null )
                    {
                        sink.success( summary );
                    }
                    else
                    {
                        sink.error( error );
                    }
                } );
            }
            else
            {
                Throwable error = Futures.completionExceptionCause( completionError );
                sink.error( error );
            }
        } ) );
    }

    // For testing purpose
    Supplier<CompletionStage<RxResultCursor>> cursorFutureSupplier()
    {
        return this.cursorFutureSupplier;
    }
}
