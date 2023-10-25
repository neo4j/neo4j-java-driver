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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.internal.async.UnmanagedTransaction;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.internal.util.QueryKeys;

public class RunResponseHandler implements ResponseHandler {
    private final CompletableFuture<Void> runFuture;
    private final MetadataExtractor metadataExtractor;
    private long queryId = MetadataExtractor.ABSENT_QUERY_ID;

    private QueryKeys queryKeys = QueryKeys.empty();
    private long resultAvailableAfter = -1;

    private final Connection connection;
    private final UnmanagedTransaction tx;

    public RunResponseHandler(
            CompletableFuture<Void> runFuture,
            MetadataExtractor metadataExtractor,
            Connection connection,
            UnmanagedTransaction tx) {
        this.runFuture = runFuture;
        this.metadataExtractor = metadataExtractor;
        this.connection = connection;
        this.tx = tx;
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        queryKeys = metadataExtractor.extractQueryKeys(metadata);
        resultAvailableAfter = metadataExtractor.extractResultAvailableAfter(metadata);
        queryId = metadataExtractor.extractQueryId(metadata);

        runFuture.complete(null);
    }

    @Override
    @SuppressWarnings("ThrowableNotThrown")
    public void onFailure(Throwable error) {
        if (tx != null) {
            tx.markTerminated(error);
        } else if (error instanceof AuthorizationExpiredException) {
            connection.terminateAndRelease(AuthorizationExpiredException.DESCRIPTION);
        } else if (error instanceof ConnectionReadTimeoutException) {
            connection.terminateAndRelease(error.getMessage());
        }
        runFuture.completeExceptionally(error);
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException();
    }

    public QueryKeys queryKeys() {
        return queryKeys;
    }

    public long resultAvailableAfter() {
        return resultAvailableAfter;
    }

    public long queryId() {
        return queryId;
    }
}
