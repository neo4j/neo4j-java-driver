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
package org.neo4j.driver.summary;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import org.neo4j.driver.internal.summary.InternalNotificationFilterConfig;

/**
 * Returns specific instances of {@link NotificationFilterConfig}.
 */
public final class NotificationFilterConfigs {
    private static final NotificationFilterConfig NONE = new InternalNotificationFilterConfig(Collections.emptySet());
    private static final NotificationFilterConfig SERVER_DEFAULTS = new InternalNotificationFilterConfig(null);

    private NotificationFilterConfigs() {}

    /**
     * Returns configuration that disables notifications.
     *
     * @return notification filter configuration
     */
    public static NotificationFilterConfig none() {
        return NONE;
    }

    /**
     * Returns configuration that uses notifications produced by server by default.
     * @return notification filter configuration
     */
    public static NotificationFilterConfig serverDefaults() {
        return SERVER_DEFAULTS;
    }

    /**
     * Returns configuration that defines a selection of {@link org.neo4j.driver.summary.NotificationFilter} instances.
     * @param filters filters to use
     * @return notification filter configuration
     */
    public static NotificationFilterConfig selection(Set<NotificationFilter> filters) {
        Objects.requireNonNull(filters, "filters must not be null");
        if (filters.isEmpty()) {
            throw new IllegalArgumentException("filters must not be empty");
        }
        return new InternalNotificationFilterConfig(filters);
    }
}
