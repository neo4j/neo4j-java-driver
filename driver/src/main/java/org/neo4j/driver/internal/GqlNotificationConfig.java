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

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.NotificationSeverity;

public record GqlNotificationConfig(
        NotificationSeverity minimumSeverity, Set<NotificationClassification> disabledClassifications) {
    public static GqlNotificationConfig defaultConfig() {
        return new GqlNotificationConfig(null, null);
    }

    public static GqlNotificationConfig from(NotificationConfig notificationConfig) {
        Objects.requireNonNull(notificationConfig);
        var config = (InternalNotificationConfig) notificationConfig;
        var disabledClassifications = config.disabledCategories() != null
                ? config.disabledCategories().stream()
                        .map(NotificationClassification.class::cast)
                        .collect(Collectors.toUnmodifiableSet())
                : null;
        return new GqlNotificationConfig(config.minimumSeverity(), disabledClassifications);
    }
}
