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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v4;

import static org.neo4j.driver.Values.value;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.ClusterComposition;
import org.neo4j.driver.internal.bolt.api.DatabaseName;
import org.neo4j.driver.internal.bolt.api.DatabaseNameUtil;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.api.summary.PullSummary;
import org.neo4j.driver.internal.bolt.api.summary.RouteSummary;
import org.neo4j.driver.internal.bolt.api.summary.RunSummary;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.PullResponseHandlerImpl;
import org.neo4j.driver.internal.bolt.basicimpl.handlers.RunResponseHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.PullMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.PullMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v3.BoltProtocolV3;
import org.neo4j.driver.internal.bolt.basicimpl.spi.Connection;
import org.neo4j.driver.types.MapAccessor;

public class BoltProtocolV4 extends BoltProtocolV3 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(4, 0);
    public static final BoltProtocol INSTANCE = new BoltProtocolV4();
    private static final String ROUTING_CONTEXT = "context";
    private static final String DATABASE_NAME = "database";
    private static final String MULTI_DB_GET_ROUTING_TABLE =
            String.format("CALL dbms.routing.getRoutingTable($%s, $%s)", ROUTING_CONTEXT, DATABASE_NAME);

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV4();
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public CompletionStage<Void> route(
            Connection connection,
            Map<String, Value> routingContext,
            Set<String> bookmarks,
            String databaseName,
            String impersonatedUser,
            MessageHandler<RouteSummary> handler,
            Clock clock,
            LoggingProvider logging) {
        var parameters = new HashMap<String, Value>();
        parameters.put(ROUTING_CONTEXT, value(routingContext));
        parameters.put(DATABASE_NAME, value((Object) databaseName));
        var query = new Query(MULTI_DB_GET_ROUTING_TABLE, parameters);

        var runMessage = RunWithMetadataMessage.autoCommitTxRunMessage(
                query.query(),
                query.parameters(),
                null,
                Collections.emptyMap(),
                DatabaseNameUtil.database("system"),
                AccessMode.READ,
                bookmarks,
                null,
                NotificationConfig.defaultConfig(),
                useLegacyNotifications(),
                logging);
        var runFuture = new CompletableFuture<RunSummary>();
        var runHandler = new RunResponseHandler(runFuture, METADATA_EXTRACTOR);

        var pullFuture = new CompletableFuture<PullSummary>();
        var records = new ArrayList<Record>();

        runFuture
                .thenCompose(ignored -> pullFuture)
                .thenApply(ignored -> {
                    var map = records.get(0);
                    var ttl = map.get("ttl").asLong();
                    var expirationTimestamp = clock.millis() + ttl * 1000;
                    if (ttl < 0 || ttl >= Long.MAX_VALUE / 1000L || expirationTimestamp < 0) {
                        expirationTimestamp = Long.MAX_VALUE;
                    }

                    Set<BoltServerAddress> readers = new LinkedHashSet<>();
                    Set<BoltServerAddress> writers = new LinkedHashSet<>();
                    Set<BoltServerAddress> routers = new LinkedHashSet<>();

                    for (var serversMap : map.get("servers").asList(MapAccessor::asMap)) {
                        var role = (Values.value(serversMap.get("role")).asString());
                        for (var server :
                                Values.value(serversMap.get("addresses")).asList()) {
                            var address =
                                    new BoltServerAddress(Values.value(server).asString());
                            switch (role) {
                                case "WRITE" -> writers.add(address);
                                case "READ" -> readers.add(address);
                                case "ROUTE" -> routers.add(address);
                            }
                        }
                    }
                    var db = map.get("db");
                    var name = db != null ? db.computeOrDefault(Value::asString, null) : null;

                    var clusterComposition =
                            new ClusterComposition(expirationTimestamp, readers, writers, routers, name);
                    return new RouteSummaryImpl(clusterComposition);
                })
                .whenComplete((summary, throwable) -> {
                    if (throwable != null) {
                        handler.onError(throwable);
                    } else {
                        handler.onSummary(summary);
                    }
                });

        return connection.write(runMessage, runHandler).thenCompose(ignored -> {
            var pullMessage = new PullMessage(-1, -1);
            var pullHandler = new PullResponseHandlerImpl(new PullMessageHandler() {
                @Override
                public void onRecord(Value[] fields) {
                    var keys = runFuture.join().keys();
                    records.add(new InternalRecord(keys, fields));
                }

                @Override
                public void onError(Throwable throwable) {
                    pullFuture.completeExceptionally(throwable);
                }

                @Override
                public void onSummary(PullSummary success) {
                    pullFuture.complete(success);
                }
            });
            return connection.write(pullMessage, pullHandler);
        });
    }

    @Override
    public CompletionStage<Void> pull(Connection connection, long qid, long request, PullMessageHandler handler) {
        var pullMessage = new PullMessage(request, qid);
        var pullFuture = new CompletableFuture<PullSummary>();
        var pullHandler = new PullResponseHandlerImpl(handler);
        return connection.write(pullMessage, pullHandler);
    }

    @Override
    protected void verifyDatabaseNameBeforeTransaction(DatabaseName databaseName) {
        // Bolt V4 accepts database name
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    private record RouteSummaryImpl(ClusterComposition clusterComposition) implements RouteSummary {}
}
