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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.request;

import static java.util.Collections.unmodifiableMap;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.Message;

/**
 * From the application point of view it is not interesting to know about the role a member plays in the cluster. Instead, the application needs to know which
 * instance can provide the wanted service.
 * <p>
 * This message is used to fetch this routing information.
 */
public record RouteMessage(
        Map<String, Value> routingContext, Set<String> bookmarks, String databaseName, String impersonatedUser)
        implements Message {
    public static final byte SIGNATURE = 0x66;

    /**
     * Constructor
     *
     * @param routingContext   The routing context used to define the routing table. Multi-datacenter deployments is one of its use cases.
     * @param bookmarks        The bookmarks used when getting the routing table.
     * @param databaseName     The name of the database to get the routing table for.
     * @param impersonatedUser The name of the impersonated user to get the routing table for, should be {@code null} for non-impersonated requests
     */
    public RouteMessage(
            Map<String, Value> routingContext, Set<String> bookmarks, String databaseName, String impersonatedUser) {
        this.routingContext = unmodifiableMap(routingContext);
        this.bookmarks = bookmarks;
        this.databaseName = databaseName;
        this.impersonatedUser = impersonatedUser;
    }

    @Override
    public byte signature() {
        return SIGNATURE;
    }

    @Override
    public String toString() {
        return String.format("ROUTE %s %s %s %s", routingContext, bookmarks, databaseName, impersonatedUser);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (RouteMessage) o;
        return routingContext.equals(that.routingContext)
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(impersonatedUser, that.impersonatedUser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(routingContext, databaseName, impersonatedUser);
    }
}
