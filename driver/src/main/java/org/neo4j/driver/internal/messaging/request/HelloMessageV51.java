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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.neo4j.driver.Value;

public class HelloMessageV51 extends HelloMessage {
    private static final String AUTH_KEY = "auth";
    private static final String EXTRA_KEY = "extra";
    private static final String NOTIFICATIONS_KEY = "notifications";

    public HelloMessageV51(
            String userAgent, Map<String, Value> authToken, Map<String, String> routingContext, Set<String> filters) {
        super(buildMetadata(userAgent, authToken, routingContext, filters));
    }

    private static Map<String, Value> buildMetadata(
            String userAgent, Map<String, Value> authToken, Map<String, String> routingContext, Set<String> filters) {
        var extras = new HashMap<>();
        extras.put(USER_AGENT_METADATA_KEY, value(userAgent));
        if (routingContext != null) {
            extras.put(ROUTING_CONTEXT_METADATA_KEY, value(routingContext));
        }
        if (filters != null) {
            extras.put(NOTIFICATIONS_KEY, value(filters));
        }
        return Map.of(AUTH_KEY, value(authToken), EXTRA_KEY, value(extras));
    }
}
