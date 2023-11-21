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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.util.MetadataExtractor;

public class CommitTxResponseHandler implements ResponseHandler {
    private final CompletableFuture<DatabaseBookmark> commitFuture;

    public CommitTxResponseHandler(CompletableFuture<DatabaseBookmark> commitFuture) {
        this.commitFuture = requireNonNull(commitFuture);
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        commitFuture.complete(MetadataExtractor.extractDatabaseBookmark(metadata));
    }

    @Override
    public void onFailure(Throwable error) {
        commitFuture.completeExceptionally(error);
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException(
                "Transaction commit is not expected to receive records: " + Arrays.toString(fields));
    }
}
