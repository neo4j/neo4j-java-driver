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
package org.neo4j.driver.stress;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.summary.ResultSummary;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AsyncWrongQueryWithRetries<C extends AbstractContext> extends AbstractAsyncQuery<C>
{
    public AsyncWrongQueryWithRetries( Driver driver )
    {
        super( driver, false );
    }

    @Override
    public CompletionStage<Void> execute( C context )
    {
        AsyncSession session = newSession( AccessMode.READ, context );

        AtomicReference<Record> recordRef = new AtomicReference<>();
        AtomicReference<Throwable> throwableRef = new AtomicReference<>();

        CompletionStage<ResultSummary> txStage = session.readTransactionAsync(
                tx -> tx.runAsync( "RETURN Wrong" )
                        .thenCompose(
                                cursor -> cursor.nextAsync()
                                                .thenCompose(
                                                        record ->
                                                        {
                                                            recordRef.set( record );
                                                            return cursor.consumeAsync();
                                                        } ) ) );

        CompletionStage<Void> resultsProcessingStage = txStage
                .handle( ( resultSummary, throwable ) ->
                         {
                             throwableRef.set( throwable );
                             return null;
                         } )
                .thenApply( nothing ->
                            {
                                assertNull( recordRef.get() );

                                Throwable cause = Futures.completionExceptionCause( throwableRef.get() );
                                assertNotNull( cause );
                                assertThat( cause, instanceOf( ClientException.class ) );
                                assertThat( ((Neo4jException) cause).code(), containsString( "SyntaxError" ) );
                                return null;
                            } );

        return resultsProcessingStage.whenComplete( ( nothing, throwable ) -> session.closeAsync() );
    }
}
