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
package org.neo4j.driver.internal.messaging.request;

import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.Value;

public class HelloMessageV51 extends HelloMessage {
    private static final String NOTIFICATIONS_KEY = "notifications";

    private final Map<String, Value> authMap;

    public HelloMessageV51(
            String userAgent,
            Map<String, Value> authToken,
            Map<String, String> routingContext,
            Value notificationFilters) {
        super(buildMetadata(userAgent, routingContext, notificationFilters));
        this.authMap = authToken;
    }

    public Map<String, Value> authMap() {
        return authMap;
    }

    private static Map<String, Value> buildMetadata(
            String userAgent, Map<String, String> routingContext, Value notificationFilters) {
        var extras = new HashMap<String, Value>();
        extras.put(USER_AGENT_METADATA_KEY, value(userAgent));
        if (routingContext != null) {
            extras.put(ROUTING_CONTEXT_METADATA_KEY, value(routingContext));
        }
        if (notificationFilters != null) {
            extras.put(NOTIFICATIONS_KEY, notificationFilters);
        }
        return extras;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        HelloMessageV51 that = (HelloMessageV51) o;
        return authMap.equals(that.authMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), authMap);
    }

    @Override
    public String toString() {
        Map<String, Map<String, Value>> dataMap = new HashMap<>();
        Map<String, Value> updatedAuthMap = new HashMap<>(authMap);
        updatedAuthMap.replace(CREDENTIALS_KEY, value("******"));
        dataMap.put("authMap", updatedAuthMap);
        dataMap.put("metadata", metadata());
        return "HELLO " + dataMap;
    }
}
