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
package org.neo4j.driver.react.result;

import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.AsyncPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.SessionPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.TransactionPullResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Statement;

import static org.neo4j.driver.internal.messaging.v4.BoltProtocolV4.METADATA_EXTRACTOR;

public class InternalStatementResultCursorFactory implements StatementResultCursorFactory
{
    private final Statement statement;
    private final RunResponseHandler runHandler;
    private final Connection connection;
    private final BookmarksHolder bookmarksHolder;
    private final ExplicitTransaction tx;
    public InternalStatementResultCursorFactory( RunResponseHandler runHandler, Statement statement,
            Connection connection, BookmarksHolder bookmarksHolder, ExplicitTransaction tx )
    {
        this.runHandler = runHandler;
        this.statement = statement;
        this.connection = connection;
        this.bookmarksHolder = bookmarksHolder;
        this.tx = tx;
    }

    @Override
    public InternalStatementResultCursor asyncResult()
    {
        BasicPullResponseHandler pullResponseHandler = newPullHandler( statement, runHandler, connection, bookmarksHolder, tx );
        return new AsyncStatementResultCursor( runHandler, new AsyncPullResponseHandler( pullResponseHandler ) );
    }

    @Override
    public RxStatementResultCursor rxResult()
    {
        return new RxStatementResultCursor( runHandler, newPullHandler( statement, runHandler, connection, bookmarksHolder, tx ) );
    }

    private static BasicPullResponseHandler newPullHandler( Statement statement, RunResponseHandler runHandler, Connection connection,
            BookmarksHolder bookmarksHolder, ExplicitTransaction tx )
    {
        if ( tx != null )
        {
            return new TransactionPullResponseHandler( statement, runHandler, connection, tx, METADATA_EXTRACTOR );
        }
        return new SessionPullResponseHandler( statement, runHandler, connection, bookmarksHolder, METADATA_EXTRACTOR );
    }
}
