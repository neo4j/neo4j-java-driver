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

import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.spi.Connection;

/**
 * Interface which defines the standard way to get the routing table
 */
public interface RoutingProcedureRunner {
    /**
     * Run the calls to the server
     *
     * @param connection       The connection which will be used to call the server
     * @param databaseName     The database name
     * @param bookmarks        The bookmarks used to query the routing information
     * @param impersonatedUser The impersonated user, should be {@code null} for non-impersonated requests
     * @return The routing table
     */
    CompletionStage<RoutingProcedureResponse> run(
            Connection connection, DatabaseName databaseName, Set<Bookmark> bookmarks, String impersonatedUser);
}
