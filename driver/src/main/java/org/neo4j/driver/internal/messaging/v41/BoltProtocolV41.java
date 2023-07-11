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
package org.neo4j.driver.internal.messaging.v41;

import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV4AutoPullHandler;
import static org.neo4j.driver.internal.handlers.PullHandlers.newBoltV4BasicPullHandler;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.neo4j.driver.Query;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.cursor.ResultCursorFactory;
import org.neo4j.driver.internal.cursor.ResultCursorFactoryImpl;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.messaging.v4.BoltProtocolV4;
import org.neo4j.driver.internal.messaging.v4.MessageFormatV4;
import org.neo4j.driver.internal.spi.Connection;

public class BoltProtocolV41 extends BoltProtocolV4 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(4, 1);
    public static final BoltProtocol INSTANCE = new BoltProtocolV41();

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV4();
    }

    @Override
    protected ResultCursorFactory buildResultCursorFactory(
            Connection connection,
            Query query,
            Consumer<DatabaseBookmark> bookmarkConsumer,
            UnmanagedTransaction tx,
            RunWithMetadataMessage runMessage,
            long fetchSize) {
        var runFuture = new CompletableFuture<Void>();
        var runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR, connection, tx);

        var pullAllHandler = newBoltV4AutoPullHandler(query, runHandler, connection, bookmarkConsumer, tx, fetchSize);
        var pullHandler = newBoltV4BasicPullHandler(query, runHandler, connection, bookmarkConsumer, tx);

        return new ResultCursorFactoryImpl(connection, runMessage, runHandler, runFuture, pullHandler, pullAllHandler);
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }
}
