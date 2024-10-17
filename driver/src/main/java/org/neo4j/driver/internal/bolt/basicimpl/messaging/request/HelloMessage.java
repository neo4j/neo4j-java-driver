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

import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.security.InternalAuthToken.CREDENTIALS_KEY;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.api.BoltAgent;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;

public class HelloMessage extends MessageWithMetadata {
    public static final byte SIGNATURE = 0x01;

    private static final String USER_AGENT_METADATA_KEY = "user_agent";
    private static final String BOLT_AGENT_METADATA_KEY = "bolt_agent";
    private static final String BOLT_AGENT_PRODUCT_KEY = "product";
    private static final String BOLT_AGENT_PLATFORM_KEY = "platform";
    private static final String BOLT_AGENT_LANGUAGE_KEY = "language";
    private static final String BOLT_AGENT_LANGUAGE_DETAIL_KEY = "language_details";
    private static final String ROUTING_CONTEXT_METADATA_KEY = "routing";
    private static final String PATCH_BOLT_METADATA_KEY = "patch_bolt";

    private static final String DATE_TIME_UTC_PATCH_VALUE = "utc";

    public HelloMessage(
            String userAgent,
            BoltAgent boltAgent,
            Map<String, Value> authMap,
            Map<String, String> routingContext,
            boolean includeDateTimeUtc,
            NotificationConfig notificationConfig,
            boolean legacyNotifications) {
        super(buildMetadata(
                userAgent,
                boltAgent,
                authMap,
                routingContext,
                includeDateTimeUtc,
                notificationConfig,
                legacyNotifications));
    }

    @Override
    public byte signature() {
        return SIGNATURE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (HelloMessage) o;
        return Objects.equals(metadata(), that.metadata());
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadata());
    }

    @Override
    public String toString() {
        Map<String, Value> metadataCopy = new HashMap<>(metadata());
        metadataCopy.replace(CREDENTIALS_KEY, value("******"));
        return "HELLO " + metadataCopy;
    }

    private static Map<String, Value> buildMetadata(
            String userAgent,
            BoltAgent boltAgent,
            Map<String, Value> authMap,
            Map<String, String> routingContext,
            boolean includeDateTimeUtc,
            NotificationConfig notificationConfig,
            boolean legacyNotifications) {
        Map<String, Value> result = new HashMap<>();
        for (var entry : authMap.entrySet()) {
            result.put(entry.getKey(), Values.value(entry.getValue()));
        }
        if (userAgent != null) {
            result.put(USER_AGENT_METADATA_KEY, value(userAgent));
        }
        if (boltAgent != null) {
            var boltAgentMap = toMap(boltAgent);
            result.put(BOLT_AGENT_METADATA_KEY, value(boltAgentMap));
        }
        if (routingContext != null) {
            result.put(ROUTING_CONTEXT_METADATA_KEY, value(routingContext));
        }
        if (includeDateTimeUtc) {
            result.put(PATCH_BOLT_METADATA_KEY, value(Collections.singleton(DATE_TIME_UTC_PATCH_VALUE)));
        }
        MessageWithMetadata.appendNotificationConfig(result, notificationConfig, legacyNotifications);
        return result;
    }

    private static HashMap<String, String> toMap(BoltAgent boltAgent) {
        var boltAgentMap = new HashMap<String, String>();
        boltAgentMap.put(BOLT_AGENT_PRODUCT_KEY, boltAgent.product());
        if (boltAgent.platform() != null) {
            boltAgentMap.put(BOLT_AGENT_PLATFORM_KEY, boltAgent.platform());
        }
        if (boltAgent.language() != null) {
            boltAgentMap.put(BOLT_AGENT_LANGUAGE_KEY, boltAgent.language());
        }
        if (boltAgent.languageDetails() != null) {
            boltAgentMap.put(BOLT_AGENT_LANGUAGE_DETAIL_KEY, boltAgent.languageDetails());
        }
        return boltAgentMap;
    }
}
