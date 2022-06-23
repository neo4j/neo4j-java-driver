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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.Value;

public class HelloMessage extends MessageWithMetadata {
    public static final byte SIGNATURE = 0x01;

    private static final String USER_AGENT_METADATA_KEY = "user_agent";
    private static final String ROUTING_CONTEXT_METADATA_KEY = "routing";
    private static final String PATCH_BOLT_METADATA_KEY = "patch_bolt";

    private static final String DATE_TIME_UTC_PATCH_VALUE = "utc";

    public HelloMessage(
            String userAgent,
            Map<String, Value> authToken,
            Map<String, String> routingContext,
            boolean includeDateTimeUtc) {
        super(buildMetadata(userAgent, authToken, routingContext, includeDateTimeUtc));
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
        HelloMessage that = (HelloMessage) o;
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
            Map<String, Value> authToken,
            Map<String, String> routingContext,
            boolean includeDateTimeUtc) {
        Map<String, Value> result = new HashMap<>(authToken);
        result.put(USER_AGENT_METADATA_KEY, value(userAgent));
        if (routingContext != null) {
            result.put(ROUTING_CONTEXT_METADATA_KEY, value(routingContext));
        }
        if (includeDateTimeUtc) {
            result.put(PATCH_BOLT_METADATA_KEY, value(Collections.singleton(DATE_TIME_UTC_PATCH_VALUE)));
        }
        return result;
    }
}
