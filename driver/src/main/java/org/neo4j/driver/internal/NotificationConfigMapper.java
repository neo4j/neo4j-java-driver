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
package org.neo4j.driver.internal;

import java.util.stream.Collectors;
import org.neo4j.bolt.connection.NotificationClassification;
import org.neo4j.bolt.connection.NotificationConfig;
import org.neo4j.bolt.connection.NotificationSeverity;
import org.neo4j.driver.NotificationCategory;

public class NotificationConfigMapper {
    public static NotificationConfig map(org.neo4j.driver.NotificationConfig config) {
        var original = (InternalNotificationConfig) config;
        var disabledCategories = original.disabledCategories();
        return new NotificationConfig(
                map(original.minimumSeverity()),
                disabledCategories != null
                        ? disabledCategories.stream()
                                .map(NotificationConfigMapper::map)
                                .collect(Collectors.toSet())
                        : null);
    }

    private static NotificationSeverity map(org.neo4j.driver.NotificationSeverity severity) {
        if (severity == null) {
            return null;
        }
        var original = (InternalNotificationSeverity) severity;
        return switch (original.type()) {
            case INFORMATION -> NotificationSeverity.INFORMATION;
            case WARNING -> NotificationSeverity.WARNING;
            case OFF -> NotificationSeverity.OFF;
        };
    }

    private static NotificationClassification map(NotificationCategory category) {
        if (category == null) {
            return null;
        }
        var original = (org.neo4j.driver.NotificationClassification) category;
        return switch (original) {
            case HINT -> new NotificationClassification(NotificationClassification.Type.HINT);
            case UNRECOGNIZED -> new NotificationClassification(NotificationClassification.Type.UNRECOGNIZED);
            case UNSUPPORTED -> new NotificationClassification(NotificationClassification.Type.UNSUPPORTED);
            case PERFORMANCE -> new NotificationClassification(NotificationClassification.Type.PERFORMANCE);
            case DEPRECATION -> new NotificationClassification(NotificationClassification.Type.DEPRECATION);
            case SECURITY -> new NotificationClassification(NotificationClassification.Type.SECURITY);
            case TOPOLOGY -> new NotificationClassification(NotificationClassification.Type.TOPOLOGY);
            case GENERIC -> new NotificationClassification(NotificationClassification.Type.GENERIC);
            case SCHEMA -> new NotificationClassification(NotificationClassification.Type.SCHEMA);
        };
    }
}
