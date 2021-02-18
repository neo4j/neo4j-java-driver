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

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AsyncReadQueryInTx<C extends AbstractContext> extends AbstractAsyncQuery<C>
{
    public AsyncReadQueryInTx( Driver driver, boolean useBookmark )
    {
        super( driver, useBookmark );
    }

    @Override
    public CompletionStage<Void> execute( C ctx )
    {
        AsyncSession session = newSession( AccessMode.READ, ctx );

        CompletionStage<Void> txCommitted = session.beginTransactionAsync()
                .thenCompose( tx -> tx.runAsync( "MATCH (n) RETURN n LIMIT 1" )
                        .thenCompose( cursor -> cursor.nextAsync()
                                .thenCompose( record -> processRecordAndGetSummary( record, cursor )
                                        .thenCompose( summary -> processSummaryAndCommit( summary, tx, ctx ) ) ) ) );

        txCommitted.whenComplete( ( ignore, error ) -> session.closeAsync() );

        return txCommitted;
    }

    private CompletionStage<ResultSummary> processRecordAndGetSummary( Record record, ResultCursor cursor )
    {
        if ( record != null )
        {
            Node node = record.get( 0 ).asNode();
            assertNotNull( node );
        }
        return cursor.consumeAsync();
    }

    private CompletionStage<Void> processSummaryAndCommit( ResultSummary summary, AsyncTransaction tx, C context )
    {
        context.readCompleted( summary );
        return tx.commitAsync();
    }
}
