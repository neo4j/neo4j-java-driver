/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.handlers;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.function.Consumer;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;

public class SessionPullResponseCompletionListener implements PullResponseCompletionListener {
    private final Consumer<DatabaseBookmark> bookmarkConsumer;
    private final Connection connection;

    public SessionPullResponseCompletionListener(Connection connection, Consumer<DatabaseBookmark> bookmarkConsumer) {
        this.bookmarkConsumer = requireNonNull(bookmarkConsumer);
        this.connection = requireNonNull(connection);
    }

    @Override
    public void afterSuccess(Map<String, Value> metadata) {
        releaseConnection();
        bookmarkConsumer.accept(MetadataExtractor.extractDatabaseBookmark(metadata));
    }

    @Override
    public void afterFailure(Throwable error) {
        if (error instanceof AuthorizationExpiredException) {
            connection.terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
        } else if (error instanceof ConnectionReadTimeoutException) {
            connection.terminateAndRelease(error.getMessage());
        } else {
            releaseConnection();
        }
    }

    private void releaseConnection() {
        connection.release(); // release in background
    }
}
