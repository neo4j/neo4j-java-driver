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

import lombok.AccessLevel;
import lombok.Getter;
import neo4j.org.testkit.backend.messages.requests.TestkitCallbackResult;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.cluster.RoutingTableRegistry;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxTransaction;

@Getter
public class TestkitState
{
    private static final String TRANSACTION_NOT_FOUND_MESSAGE = "Could not find transaction";

    private final Map<String,Driver> drivers = new HashMap<>();
    private final Map<String,RoutingTableRegistry> routingTableRegistry = new HashMap<>();
    private final Map<String,SessionState> sessionStates = new HashMap<>();
    private final Map<String,AsyncSessionState> asyncSessionStates = new HashMap<>();
    private final Map<String,RxSessionState> rxSessionStates = new HashMap<>();
    private final Map<String,Result> results = new HashMap<>();
    private final Map<String,ResultCursor> resultCursors = new HashMap<>();
    private final Map<String,RxResult> rxResults = new HashMap<>();
    private final Map<String,RxBlockingSubscriber<Record>> rxResultIdToRecordSubscriber = new HashMap<>();
    @Getter( AccessLevel.NONE )
    private final Map<String,Transaction> transactions = new HashMap<>();
    @Getter( AccessLevel.NONE )
    private final Map<String,AsyncTransaction> asyncTransactions = new HashMap<>();
    @Getter( AccessLevel.NONE )
    private final Map<String,RxTransaction> rxTransactions = new HashMap<>();
    private final Map<String,Neo4jException> errors = new HashMap<>();
    @Getter( AccessLevel.NONE )
    private final AtomicInteger idGenerator = new AtomicInteger( 0 );
    private final Consumer<TestkitResponse> responseWriter;
    private final Map<String,CompletableFuture<TestkitCallbackResult>> callbackIdToFuture = new HashMap<>();

    public TestkitState( Consumer<TestkitResponse> responseWriter )
    {
        this.responseWriter = responseWriter;
    }

    public String newId()
    {
        return String.valueOf( idGenerator.getAndIncrement() );
    }

    public String addTransaction( Transaction transaction )
    {
        String id = newId();
        this.transactions.put( id, transaction );
        return id;
    }

    public Transaction getTransaction( String id )
    {
        if ( !this.transactions.containsKey( id ) )
        {
            throw new RuntimeException( TRANSACTION_NOT_FOUND_MESSAGE );
        }
        return this.transactions.get( id );
    }

    public String addAsyncTransaction( AsyncTransaction transaction )
    {
        String id = newId();
        this.asyncTransactions.put( id, transaction );
        return id;
    }

    public CompletableFuture<AsyncTransaction> getAsyncTransaction( String id )
    {
        if ( !this.asyncTransactions.containsKey( id ) )
        {
            CompletableFuture<AsyncTransaction> future = new CompletableFuture<>();
            future.completeExceptionally( new RuntimeException( TRANSACTION_NOT_FOUND_MESSAGE ) );
            return future;
        }
        return CompletableFuture.completedFuture( asyncTransactions.get( id ) );
    }

    public String addRxTransaction( RxTransaction transaction )
    {
        String id = newId();
        this.rxTransactions.put( id, transaction );
        return id;
    }

    public Mono<RxTransaction> getRxTransaction( String id )
    {
        if ( !this.rxTransactions.containsKey( id ) )
        {
            return Mono.error( new RuntimeException( TRANSACTION_NOT_FOUND_MESSAGE ) );
        }
        return Mono.just( rxTransactions.get( id ) );
    }
}
