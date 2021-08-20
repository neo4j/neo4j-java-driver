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
package neo4j.org.testkit.backend;

import lombok.Getter;
import neo4j.org.testkit.backend.messages.requests.TestkitCallbackResult;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.cluster.RoutingTableRegistry;

@Getter
public class TestkitState
{
    private final Map<String,Driver> drivers = new HashMap<>();
    private final Map<String,RoutingTableRegistry> routingTableRegistry = new HashMap<>();
    private final Map<String,SessionState> sessionStates = new HashMap<>();
    private final Map<String,AsyncSessionState> asyncSessionStates = new HashMap<>();
    private final Map<String,Result> results = new HashMap<>();
    private final Map<String,ResultCursor> resultCursors = new HashMap<>();
    private final Map<String,Transaction> transactions = new HashMap<>();
    private final Map<String,AsyncTransaction> asyncTransactions = new HashMap<>();
    private final Map<String,Neo4jException> errors = new HashMap<>();
    private int idGenerator = 0;
    private final Consumer<TestkitResponse> responseWriter;
    private final Supplier<Boolean> processor;
    private final Map<String,CompletableFuture<TestkitCallbackResult>> callbackIdToFuture = new HashMap<>();

    public TestkitState( Consumer<TestkitResponse> responseWriter, Supplier<Boolean> processor )
    {
        this.responseWriter = responseWriter;
        this.processor = processor;
    }

    public CompletionStage<TestkitCallbackResult> dispatchTestkitCallback( TestkitCallback response )
    {
        CompletableFuture<TestkitCallbackResult> future = new CompletableFuture<>();
        callbackIdToFuture.put( response.getCallbackId(), future );
        responseWriter.accept( response );
        // This is required for sync backend, but should be removed during migration to Netty implementation.
        processor.get();
        return future;
    }

    public String newId()
    {
        return String.valueOf( idGenerator++ );
    }
}
