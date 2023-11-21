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

import static java.lang.String.format;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.supportsMultiDatabase;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.supportsRouteMessage;

import java.time.Clock;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.exceptions.value.ValueException;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.spi.Connection;

public class RoutingProcedureClusterCompositionProvider implements ClusterCompositionProvider {
    private static final String PROTOCOL_ERROR_MESSAGE = "Failed to parse '%s' result received from server due to ";

    private final Clock clock;
    private final RoutingProcedureRunner singleDatabaseRoutingProcedureRunner;
    private final RoutingProcedureRunner multiDatabaseRoutingProcedureRunner;
    private final RoutingProcedureRunner routeMessageRoutingProcedureRunner;

    public RoutingProcedureClusterCompositionProvider(Clock clock, RoutingContext routingContext, Logging logging) {
        this(
                clock,
                new SingleDatabaseRoutingProcedureRunner(routingContext, logging),
                new MultiDatabasesRoutingProcedureRunner(routingContext, logging),
                new RouteMessageRoutingProcedureRunner(routingContext));
    }

    RoutingProcedureClusterCompositionProvider(
            Clock clock,
            SingleDatabaseRoutingProcedureRunner singleDatabaseRoutingProcedureRunner,
            MultiDatabasesRoutingProcedureRunner multiDatabaseRoutingProcedureRunner,
            RouteMessageRoutingProcedureRunner routeMessageRoutingProcedureRunner) {
        this.clock = clock;
        this.singleDatabaseRoutingProcedureRunner = singleDatabaseRoutingProcedureRunner;
        this.multiDatabaseRoutingProcedureRunner = multiDatabaseRoutingProcedureRunner;
        this.routeMessageRoutingProcedureRunner = routeMessageRoutingProcedureRunner;
    }

    @Override
    public CompletionStage<ClusterComposition> getClusterComposition(
            Connection connection, DatabaseName databaseName, Set<Bookmark> bookmarks, String impersonatedUser) {
        RoutingProcedureRunner runner;

        if (supportsRouteMessage(connection)) {
            runner = routeMessageRoutingProcedureRunner;
        } else if (supportsMultiDatabase(connection)) {
            runner = multiDatabaseRoutingProcedureRunner;
        } else {
            runner = singleDatabaseRoutingProcedureRunner;
        }

        return runner.run(connection, databaseName, bookmarks, impersonatedUser)
                .thenApply(this::processRoutingResponse);
    }

    private ClusterComposition processRoutingResponse(RoutingProcedureResponse response) {
        if (!response.isSuccess()) {
            throw new CompletionException(
                    format(
                            "Failed to run '%s' on server. Please make sure that there is a Neo4j server or cluster up running.",
                            invokedProcedureString(response)),
                    response.error());
        }

        var records = response.records();

        var now = clock.millis();

        // the record size is wrong
        if (records.size() != 1) {
            throw new ProtocolException(format(
                    PROTOCOL_ERROR_MESSAGE + "records received '%s' is too few or too many.",
                    invokedProcedureString(response),
                    records.size()));
        }

        // failed to parse the record
        ClusterComposition cluster;
        try {
            cluster = ClusterComposition.parse(records.get(0), now);
        } catch (ValueException e) {
            throw new ProtocolException(
                    format(PROTOCOL_ERROR_MESSAGE + "unparsable record received.", invokedProcedureString(response)),
                    e);
        }

        // the cluster result is not a legal reply
        if (!cluster.hasRoutersAndReaders()) {
            throw new ProtocolException(format(
                    PROTOCOL_ERROR_MESSAGE + "no router or reader found in response.",
                    invokedProcedureString(response)));
        }

        // all good
        return cluster;
    }

    private static String invokedProcedureString(RoutingProcedureResponse response) {
        var query = response.procedure();
        return query.text() + " " + query.parameters();
    }
}
