/*
 * Copyright (c) 2002-2009 "Neo4j,"
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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.react.result.RxStatementResultCursor;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.summary.ResultSummary;

public class InternalRxResult implements RxResult
{
    private final CompletionStage<RxStatementResultCursor> cursorFuture;
    private final CompletableFuture<ResultSummary> summaryFuture = new CompletableFuture<>();

    public InternalRxResult( CompletionStage<RxStatementResultCursor> cursorFuture )
    {
        this.cursorFuture = cursorFuture;
        this.cursorFuture.whenComplete( ( cursor, error ) -> {
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
            }
            else
            {
                summaryFuture.completeExceptionally( error );
            }
        } );
    }

    @Override
    public Publisher<String> keys()
    {
        return Flux.create( sink -> {
            cursorFuture.whenComplete( ( cursor, error ) -> {
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
        return Flux.create( sink -> cursorFuture.whenComplete( ( cursor, error ) -> {
            if ( error != null )
            {
                sink.error( error );
            }
            else
            {
                sink.onCancel( cursor::cancel );
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
                sink.onRequest( cursor::request );
            }
        } ) );
    }

    @Override
    public Publisher<ResultSummary> summary()
    {
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
}
