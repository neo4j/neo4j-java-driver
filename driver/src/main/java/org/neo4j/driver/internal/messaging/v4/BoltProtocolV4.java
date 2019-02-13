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
package org.neo4j.driver.internal.messaging.v4;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.BookmarksHolder;
import org.neo4j.driver.internal.ExplicitTransaction;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.SessionPullResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.TransactionPullResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.react.internal.cursor.InternalStatementResultCursorFactory;
import org.neo4j.driver.react.internal.cursor.StatementResultCursorFactory;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.Value;

import static org.neo4j.driver.v1.Values.ofValue;

public class BoltProtocolV4 extends BoltProtocolV3
{
    public static final int VERSION = 4;
    public static final BoltProtocol INSTANCE = new BoltProtocolV4();

    @Override
    public MessageFormat createMessageFormat()
    {
        return new MessageFormatV4();
    }

    protected StatementResultCursorFactory buildResultCursorFactory( Connection connection, Statement statement, BookmarksHolder bookmarksHolder,
            ExplicitTransaction tx, TransactionConfig config, boolean waitForRunResponse )
    {
        CompletableFuture<Throwable> runCompletedFuture = new CompletableFuture<>();

        String query = statement.text();
        Map<String,Value> params = statement.parameters().asMap( ofValue() );
        Message runMessage = new RunWithMetadataMessage( query, params, bookmarksHolder.getBookmarks(), config, connection.mode() );
        RunResponseHandler runHandler = new RunResponseHandler( runCompletedFuture, METADATA_EXTRACTOR );

        BasicPullResponseHandler pullHandler = newPullHandler( statement, runHandler, connection, bookmarksHolder, tx );

        return new InternalStatementResultCursorFactory( connection, runMessage, runHandler, pullHandler, waitForRunResponse );
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
