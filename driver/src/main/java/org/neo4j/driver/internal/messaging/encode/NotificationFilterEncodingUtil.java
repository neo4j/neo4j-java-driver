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
package org.neo4j.driver.internal.messaging.encode;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.summary.NotificationFilter;

public class NotificationFilterEncodingUtil {
    private static final String ENCODED_FILTER_PATTERN = "%s.%s";

    public static Value encode(Set<NotificationFilter> filters, boolean encodeNullExplicitly) {
        Value value;
        if (filters == null) {
            value = encodeNullExplicitly ? Values.NULL : null;
        } else if (filters.isEmpty()) {
            value = Values.value(Collections.emptySet());
        } else {
            value = Values.value(filters.stream()
                    .map(NotificationFilterEncodingUtil::encodeNotificationFilter)
                    .collect(Collectors.toSet()));
        }
        return value;
    }

    private static String encodeNotificationFilter(NotificationFilter filter) {
        return String.format(
                ENCODED_FILTER_PATTERN,
                encodeString(filter.severity().name()),
                encodeString(filter.category().name()));
    }

    private static String encodeString(String value) {
        return value.equals("ALL") ? "*" : value;
    }
}
