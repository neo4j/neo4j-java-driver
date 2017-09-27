/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1.stress;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.types.Node;

import static org.junit.Assert.assertNotNull;

public class AsyncReadQueryInTx<C extends AbstractContext> extends AbstractAsyncQuery<C>
{
    public AsyncReadQueryInTx( Driver driver, boolean useBookmark )
    {
        super( driver, useBookmark );
    }

    @Override
    public CompletionStage<Void> execute( C ctx )
    {
        Session session = newSession( AccessMode.READ, ctx );

        CompletionStage<Void> txCommitted = session.beginTransactionAsync()
                .thenCompose( tx -> tx.runAsync( "MATCH (n) RETURN n LIMIT 1" )
                        .thenCompose( cursor -> cursor.nextAsync()
                                .thenCompose( record -> processRecordAndGetSummary( record, cursor )
                                        .thenCompose( summary -> processSummaryAndCommit( summary, tx, ctx ) ) ) ) );

        txCommitted.whenComplete( ( ignore, error ) -> session.closeAsync() );

        return txCommitted;
    }

    private CompletionStage<ResultSummary> processRecordAndGetSummary( Record record, StatementResultCursor cursor )
    {
        if ( record != null )
        {
            Node node = record.get( 0 ).asNode();
            assertNotNull( node );
        }
        return cursor.summaryAsync();
    }

    private CompletionStage<Void> processSummaryAndCommit( ResultSummary summary, Transaction tx, C context )
    {
        context.readCompleted( summary );
        return tx.commitAsync();
    }
}
