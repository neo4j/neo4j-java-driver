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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.internal.cursor.RxStatementResultCursor;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

import static org.neo4j.driver.internal.handlers.pulln.AbstractBasicPullResponseHandler.DISCARD_RECORD_CONSUMER;

public class InternalRxResult implements RxResult
{
    private Supplier<CompletionStage<RxStatementResultCursor>> cursorFutureSupplier;
    private volatile CompletionStage<RxStatementResultCursor> cursorFuture;
    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();

    public InternalRxResult( Supplier<CompletionStage<RxStatementResultCursor>> cursorFuture )
    {
        this.cursorFutureSupplier = cursorFuture;
    }

    @Override
    public Publisher<String> keys()
    {
        return Flux.create( sink -> {
            getCursorFuture().whenComplete( ( cursor, completionError ) -> {
                if ( cursor != null )
                {
                    List<String> keys = cursor.keys();

                    Iterator<String> iterator = keys.iterator();
                    sink.onRequest( n -> {
                        while ( n-- > 0 )
                        {
                            if ( iterator.hasNext() )
                            {
                                sink.next( iterator.next() );
                            }
                            else
                            {
                                sink.complete();
                                break;
                            }
                        }
                    } );
                    sink.onCancel( sink::complete );
                }
                else
                {
                    Throwable error = Futures.completionExceptionCause( completionError );
                    sink.error( error );
                }
            } );
        } );
    }

    @Override
    public Publisher<Record> records()
    {
        return Flux.create( sink -> getCursorFuture().whenComplete( ( cursor, completionError ) -> {
            if( cursor != null )
            {
                if( summaryFuture.isDone() )
                {
                    sink.complete();
                }
                else
                {
                    cursor.installRecordConsumer( ( r, e ) -> {
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
                    } );
                    sink.onCancel( cursor::cancel );
                    sink.onRequest( cursor::request );
                }
            }
            else
            {
                Throwable error = Futures.completionExceptionCause( completionError );
                sink.error( error );
            }
        } ) );
    }

    private CompletionStage<RxStatementResultCursor> getCursorFuture()
    {
        if ( cursorFuture != null )
        {
            return cursorFuture;
        }
        return initCursorFuture();
    }

    synchronized CompletionStage<RxStatementResultCursor> initCursorFuture()
    {
        // A quick path to return
        if ( cursorFuture != null )
        {
            return cursorFuture;
        }

        // now we obtained lock and we are going to be the one who assigns cursorFuture one and only once.
        CompletableFuture<RxStatementResultCursor> cursorWithSummaryConsumerInstalled = new CompletableFuture<>();
        CompletionStage<RxStatementResultCursor> cursorFuture = cursorFutureSupplier.get();
        cursorFutureSupplier = null; // we no longer need the reference to this object
        cursorFuture.whenComplete( ( cursor, completionError ) -> {
            if ( cursor != null )
            {
                cursor.installSummaryConsumer( ( summary, summaryError ) -> {
                    if ( summaryError != null )
                    {
                        summaryFuture.completeExceptionally( summaryError );
                    }
                    else if( summary != null )
                    {
                        summaryFuture.complete( summary );
                    }
                    //else (null, null) to indicate a has_more success
                } );
                cursorWithSummaryConsumerInstalled.complete( cursor );
            }
            else
            {
                Throwable error = Futures.completionExceptionCause( completionError );
                summaryFuture.completeExceptionally( error );
                cursorWithSummaryConsumerInstalled.completeExceptionally( error );
            }
        } );

        this.cursorFuture = cursorWithSummaryConsumerInstalled;
        return this.cursorFuture;
    }

    @Override
    public Publisher<ResultSummary> summary()
    {
        return Mono.create( sink -> getCursorFuture().whenComplete( ( cursor, completionError ) -> {
            // first register callback when summary future finishes
            summaryFuture.whenComplete( ( summary, summaryCompletionError ) -> {
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

            if ( cursor != null )
            {
                if ( !summaryFuture.isDone() )
                {
                    // the summary is called before record streaming
                    cursor.installRecordConsumer( DISCARD_RECORD_CONSUMER );
                    cursor.cancel();
                }
            }
            else
            {
                Throwable error = Futures.completionExceptionCause( completionError );
                summaryFuture.completeExceptionally( error );
            }
        } ) );
    }

    // For test purpose
    Supplier<CompletionStage<RxStatementResultCursor>> cursorFutureSupplier()
    {
        return this.cursorFutureSupplier;
    }
}
