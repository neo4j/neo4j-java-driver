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
package org.neo4j.driver.internal;

import org.neo4j.driver.Statement;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.internal.async.ExplicitTransaction;
import org.neo4j.driver.internal.util.Futures;

public class InternalTransaction extends AbstractStatementRunner implements Transaction
{
    private final ExplicitTransaction tx;
    public InternalTransaction( ExplicitTransaction tx )
    {
        this.tx = tx;
    }

    @Override
    public void success()
    {
        tx.success();
    }

    @Override
    public void failure()
    {
        tx.failure();
    }

    @Override
    public void close()
    {
        Futures.blockingGet( tx.closeAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while closing the transaction" ) );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        StatementResultCursor cursor = Futures.blockingGet( tx.runAsync( statement, false ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while running query in transaction" ) );
        return new InternalStatementResult( tx.connection(), cursor );
    }

    @Override
    public boolean isOpen()
    {
        return tx.isOpen();
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        tx.connection().terminateAndRelease( reason );
    }
}
