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
package org.neo4j.driver.internal.async;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Statement;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.internal.Bookmarks;
import org.neo4j.driver.internal.cursor.RxStatementResultCursor;
import org.neo4j.driver.internal.spi.Connection;

public interface ExplicitTransaction
{
    CompletionStage<ExplicitTransaction> beginAsync( Bookmarks initialBookmarks, TransactionConfig config );

    CompletionStage<StatementResultCursor> runAsync( Statement statement, boolean waitForRunResponse );

    CompletionStage<RxStatementResultCursor> runRx( Statement statement );

    void success();

    void failure();

    CompletionStage<Void> commitAsync();

    CompletionStage<Void> rollbackAsync();

    boolean isOpen();

    CompletionStage<Void> closeAsync();

    void markTerminated();

    Connection connection();
}
