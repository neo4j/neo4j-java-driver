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
package org.neo4j.driver.internal.messaging.request;

import static org.neo4j.driver.Values.value;

import java.util.Map;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNotificationCategory;
import org.neo4j.driver.internal.InternalNotificationConfig;
import org.neo4j.driver.internal.InternalNotificationSeverity;
import org.neo4j.driver.internal.messaging.Message;

abstract class MessageWithMetadata implements Message {
    static final String NOTIFICATIONS_MINIMUM_SEVERITY = "notifications_minimum_severity";
    static final String NOTIFICATIONS_DISABLED_CATEGORIES = "notifications_disabled_categories";
    private final Map<String, Value> metadata;

    public MessageWithMetadata(Map<String, Value> metadata) {
        this.metadata = metadata;
    }

    public Map<String, Value> metadata() {
        return metadata;
    }

    static void appendNotificationConfig(Map<String, Value> result, NotificationConfig config) {
        if (config != null) {
            if (config instanceof InternalNotificationConfig internalConfig) {
                var severity = (InternalNotificationSeverity) internalConfig.minimumSeverity();
                if (severity != null) {
                    result.put(
                            NOTIFICATIONS_MINIMUM_SEVERITY,
                            value(severity.type().toString()));
                }
                var disabledCategories = internalConfig.disabledCategories();
                if (disabledCategories != null) {
                    var list = disabledCategories.stream()
                            .map(category -> (InternalNotificationCategory) category)
                            .map(category -> category.type().toString())
                            .toList();
                    result.put(NOTIFICATIONS_DISABLED_CATEGORIES, value(list));
                }
            }
        }
    }
}
