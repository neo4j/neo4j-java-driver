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
package org.neo4j.driver.internal.bolt.basicimpl.handlers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.basicimpl.spi.ResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.util.MetadataExtractor;

public class RunResponseHandler implements ResponseHandler {
    private final CompletableFuture<RunSummary> runFuture;
    private final MetadataExtractor metadataExtractor;

    public RunResponseHandler(CompletableFuture<RunSummary> runFuture, MetadataExtractor metadataExtractor) {
        this.runFuture = runFuture;
        this.metadataExtractor = metadataExtractor;
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        var queryKeys = metadataExtractor.extractQueryKeys(metadata);
        var resultAvailableAfter = metadataExtractor.extractResultAvailableAfter(metadata);
        var queryId = metadataExtractor.extractQueryId(metadata);

        runFuture.complete(new RunResponseImpl(queryId, queryKeys, resultAvailableAfter));
    }

    @Override
    public void onFailure(Throwable error) {
        runFuture.completeExceptionally(error);
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException();
    }

    private record RunResponseImpl(long queryId, List<String> keys, long resultAvailableAfter) implements RunSummary {}
}
