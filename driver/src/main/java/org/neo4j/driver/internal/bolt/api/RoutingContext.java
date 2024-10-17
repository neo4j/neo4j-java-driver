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
package org.neo4j.driver.internal.bolt.api;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.neo4j.driver.internal.bolt.api.Scheme.isRoutingScheme;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class RoutingContext {

    public static final RoutingContext EMPTY = new RoutingContext();
    private static final String ROUTING_ADDRESS_KEY = "address";

    private final Map<String, String> context;
    private final boolean isServerRoutingEnabled;

    private RoutingContext() {
        this.isServerRoutingEnabled = true;
        this.context = emptyMap();
    }

    public RoutingContext(URI uri) {
        this.isServerRoutingEnabled = isRoutingScheme(uri.getScheme());
        this.context = unmodifiableMap(parseParameters(uri));
    }

    public boolean isDefined() {
        return context.size() > 1;
    }

    public Map<String, String> toMap() {
        return context;
    }

    public boolean isServerRoutingEnabled() {
        return isServerRoutingEnabled;
    }

    @Override
    public String toString() {
        return "RoutingContext" + context + " isServerRoutingEnabled=" + isServerRoutingEnabled;
    }

    private static Map<String, String> parseParameters(URI uri) {
        var query = uri.getQuery();
        String address;

        if (uri.getPort() == -1) {
            address = String.format("%s:%s", uri.getHost(), BoltServerAddress.DEFAULT_PORT);
        } else {
            address = String.format("%s:%s", uri.getHost(), uri.getPort());
        }

        Map<String, String> parameters = new HashMap<>();
        parameters.put(ROUTING_ADDRESS_KEY, address);

        if (query == null || query.isEmpty()) {
            return parameters;
        }

        var pairs = query.split("&");
        for (var pair : pairs) {
            var keyValue = pair.split("=");
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid parameters: '" + pair + "' in URI '" + uri + "'");
            }

            var previousValue =
                    parameters.put(trimAndVerifyKey(keyValue[0], uri), trimAndVerify(keyValue[1], "value", uri));
            if (previousValue != null) {
                throw new IllegalArgumentException(
                        "Duplicated query parameters with key '" + previousValue + "' in URI '" + uri + "'");
            }
        }
        return parameters;
    }

    private static String trimAndVerifyKey(String s, URI uri) {
        var trimmed = trimAndVerify(s, "key", uri);

        if (trimmed.equals(ROUTING_ADDRESS_KEY)) {
            throw new IllegalArgumentException("The key 'address' is reserved for routing context.");
        }

        return trimmed;
    }

    private static String trimAndVerify(String string, String name, URI uri) {
        var result = string.trim();
        if (result.isEmpty()) {
            throw new IllegalArgumentException("Illegal empty " + name + " in URI query '" + uri + "'");
        }
        return result;
    }
}
