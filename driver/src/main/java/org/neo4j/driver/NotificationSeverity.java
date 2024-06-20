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
package org.neo4j.driver;

import static org.neo4j.driver.internal.InternalNotificationSeverity.Type;

import java.io.Serializable;
import org.neo4j.driver.Config.ConfigBuilder;
import org.neo4j.driver.internal.InternalNotificationSeverity;
import org.neo4j.driver.util.Preview;

/**
 * Notification severity level.
 *
 * @since 5.7
 */
public sealed interface NotificationSeverity extends Serializable, Comparable<NotificationSeverity>
        permits InternalNotificationSeverity {
    /**
     * An information severity level.
     */
    NotificationSeverity INFORMATION = new InternalNotificationSeverity(Type.INFORMATION, 800);
    /**
     * A warning severity level.
     */
    NotificationSeverity WARNING = new InternalNotificationSeverity(Type.WARNING, 900);
    /**
     * A special severity level used in configuration to turn off all notifications.
     * @since 5.22.0
     * @see ConfigBuilder#withMinimumNotificationSeverity(NotificationSeverity)
     * @see SessionConfig.Builder#withMinimumNotificationSeverity(NotificationSeverity)
     */
    @Preview(name = "GQL-status object")
    NotificationSeverity OFF = new InternalNotificationSeverity(Type.OFF, Integer.MAX_VALUE);
}
