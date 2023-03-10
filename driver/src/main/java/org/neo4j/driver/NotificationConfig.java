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
package org.neo4j.driver;

import java.io.Serializable;
import java.util.Set;
import org.neo4j.driver.internal.InternalNotificationConfig;
import org.neo4j.driver.internal.InternalNotificationSeverity;

/**
 * Notification configuration that defines what notifications the server should supply to the driver.
 * <p>
 * This configuration is only supported over Bolt protocol version 5.2 and above. It is effectively ignored on the previous versions.
 *
 * @since 5.7
 */
public sealed interface NotificationConfig extends Serializable permits InternalNotificationConfig {
    /**
     * Returns a default notification config.
     * <p>
     * It has no options activated, meaning the resulting behaviour depends on an upstream entity. For instance,
     * when this config is set on the session level, the resulting behaviour depends on the driver's config.
     * Likewise, when this config is set on the driver level, the resulting behaviour depends on the server.
     *
     * @return the default config
     */
    static NotificationConfig defaultConfig() {
        return new InternalNotificationConfig(null, null);
    }

    /**
     * A config that disables all notifications.
     *
     * @return the config that disables all notifications
     */
    static NotificationConfig disableAll() {
        return new InternalNotificationConfig(InternalNotificationSeverity.OFF, null);
    }

    /**
     * Returns a config that sets a minimum severity level for notifications.
     *
     * @param minimumSeverity the minimum severity level
     * @return the config
     */
    NotificationConfig enableMinimumSeverity(NotificationSeverity minimumSeverity);

    /**
     * Returns a config that disables a set of notification categories.
     *
     * @param disabledCategories the categories to disable, an empty set means no categories are disabled
     * @return the config
     */
    NotificationConfig disableCategories(Set<NotificationCategory> disabledCategories);
}
