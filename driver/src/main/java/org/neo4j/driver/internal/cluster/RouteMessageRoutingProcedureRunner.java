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
package org.neo4j.driver.internal.cluster;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.handlers.RouteMessageResponseHandler;
import org.neo4j.driver.internal.messaging.request.RouteMessage;
import org.neo4j.driver.internal.spi.Connection;

/**
 * This implementation of the {@link RoutingProcedureRunner} access the routing procedure
 * through the bolt's ROUTE message.
 */
public class RouteMessageRoutingProcedureRunner implements RoutingProcedureRunner {
    private final Map<String, Value> routingContext;
    private final Supplier<CompletableFuture<Map<String, Value>>> createCompletableFuture;

    public RouteMessageRoutingProcedureRunner(RoutingContext routingContext) {
        this(routingContext, CompletableFuture::new);
    }

    protected RouteMessageRoutingProcedureRunner(
            RoutingContext routingContext, Supplier<CompletableFuture<Map<String, Value>>> createCompletableFuture) {
        this.routingContext = routingContext.toMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Values.value(entry.getValue())));
        this.createCompletableFuture = createCompletableFuture;
    }

    @Override
    public CompletionStage<RoutingProcedureResponse> run(
            Connection connection, DatabaseName databaseName, Set<Bookmark> bookmarks, String impersonatedUser) {
        var completableFuture = createCompletableFuture.get();

        var directConnection = toDirectConnection(connection, databaseName, impersonatedUser);
        directConnection.writeAndFlush(
                new RouteMessage(
                        routingContext, bookmarks, databaseName.databaseName().orElse(null), impersonatedUser),
                new RouteMessageResponseHandler(completableFuture));
        return completableFuture
                .thenApply(routingTable ->
                        new RoutingProcedureResponse(getQuery(databaseName), singletonList(toRecord(routingTable))))
                .exceptionally(throwable -> new RoutingProcedureResponse(getQuery(databaseName), throwable.getCause()))
                .thenCompose(routingProcedureResponse ->
                        directConnection.release().thenApply(ignore -> routingProcedureResponse));
    }

    private Record toRecord(Map<String, Value> routingTable) {
        return new InternalRecord(
                new ArrayList<>(routingTable.keySet()), routingTable.values().toArray(new Value[0]));
    }

    private DirectConnection toDirectConnection(
            Connection connection, DatabaseName databaseName, String impersonatedUser) {
        return new DirectConnection(connection, databaseName, AccessMode.READ, impersonatedUser);
    }

    private Query getQuery(DatabaseName databaseName) {
        Map<String, Object> params = new HashMap<>();
        params.put("routingContext", routingContext);
        params.put("databaseName", databaseName.databaseName().orElse(null));
        return new Query("ROUTE $routingContext $databaseName", params);
    }
}
