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

import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.DatabaseNameUtil.systemDatabase;

import java.util.HashMap;
import java.util.Set;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.async.connection.DirectConnection;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.spi.Connection;

/**
 * This implementation of the {@link RoutingProcedureRunner} works with multi database versions of Neo4j calling
 * the procedure `dbms.routing.getRoutingTable`
 */
public class MultiDatabasesRoutingProcedureRunner extends SingleDatabaseRoutingProcedureRunner {
    static final String DATABASE_NAME = "database";
    static final String MULTI_DB_GET_ROUTING_TABLE =
            String.format("CALL dbms.routing.getRoutingTable($%s, $%s)", ROUTING_CONTEXT, DATABASE_NAME);

    public MultiDatabasesRoutingProcedureRunner(RoutingContext context, Logging logging) {
        super(context, logging);
    }

    @Override
    Set<Bookmark> adaptBookmarks(Set<Bookmark> bookmarks) {
        return bookmarks;
    }

    @Override
    Query procedureQuery(BoltProtocolVersion protocolVersion, DatabaseName databaseName) {
        var map = new HashMap<String, Value>();
        map.put(ROUTING_CONTEXT, value(context.toMap()));
        map.put(DATABASE_NAME, value((Object) databaseName.databaseName().orElse(null)));
        return new Query(MULTI_DB_GET_ROUTING_TABLE, value(map));
    }

    @Override
    DirectConnection connection(Connection connection) {
        return new DirectConnection(connection, systemDatabase(), AccessMode.READ, null);
    }
}
