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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.react.RxResult;
import org.neo4j.driver.react.internal.cursor.RxStatementResultCursor;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

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
            getCursorFuture().whenComplete( ( cursor, error ) -> {
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
                    sink.error( error );
                }
            } );
        } );
    }

    @Override
    public Publisher<Record> records()
    {
        return Flux.create( sink -> getCursorFuture().whenComplete( ( cursor, error ) -> {
            if( cursor != null )
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
            else
            {
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

    private synchronized CompletionStage<RxStatementResultCursor> initCursorFuture()
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
        cursorFuture.whenComplete( ( cursor, error ) -> {
            if ( cursor != null )
            {
                cursor.installSummaryConsumer( ( summary, summaryError ) -> {
                    if ( summary != null )
                    {
                        summaryFuture.complete( summary );
                    }
                    else if ( summaryError != null )
                    {
                        summaryFuture.completeExceptionally( summaryError );
                    }
                } );
                cursorWithSummaryConsumerInstalled.complete( cursor );
            }
            else
            {
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
        // TODO currently, summary will not start running or streaming.
        // this means if a user just call summary, he will just hanging there forever.
        return Mono.create( sink -> summaryFuture.whenComplete( ( summary, error ) -> {
            if ( error != null )
            {
                sink.error( error );
            }
            else
            {
                sink.success( summary );
            }
        } ) );
    }

    // For test purpose
    Supplier<CompletionStage<RxStatementResultCursor>> cursorFutureSupplier()
    {
        return this.cursorFutureSupplier;
    }
}
