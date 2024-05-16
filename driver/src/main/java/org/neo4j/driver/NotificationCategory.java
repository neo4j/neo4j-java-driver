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

import java.io.Serializable;

/**
 * Notification category.
 *
 * @since 5.7
 * @deprecated superseded by {@link NotificationClassification}
 */
@Deprecated
public sealed interface NotificationCategory extends Serializable permits NotificationClassification {
    /**
     * A hint category.
     * <p>
     * For instance, the given hint cannot be satisfied.
     */
    NotificationCategory HINT = NotificationClassification.HINT;

    /**
     * An unrecognized category.
     * <p>
     * For instance, the query or command mentions entities that are unknown to the system.
     */
    NotificationCategory UNRECOGNIZED = NotificationClassification.UNRECOGNIZED;

    /**
     * An unsupported category.
     * <p>
     * For instance, the query/command is trying to use features that are not supported by the current system or using
     * features that are experimental and should not be used in production.
     */
    NotificationCategory UNSUPPORTED = NotificationClassification.UNSUPPORTED;

    /**
     * A performance category.
     * <p>
     * For instance, the query uses costly operations and might be slow.
     */
    NotificationCategory PERFORMANCE = NotificationClassification.PERFORMANCE;

    /**
     * A deprecation category.
     * <p>
     * For instance, the query/command use deprecated features that should be replaced.
     */
    NotificationCategory DEPRECATION = NotificationClassification.DEPRECATION;

    /**
     * A security category.
     * <p>
     * For instance, the security warnings.
     * <p>
     * Please note that this category was added to a later server version. Therefore, a compatible server version is
     * required to use it.
     *
     * @since 5.14
     */
    NotificationCategory SECURITY = NotificationClassification.SECURITY;

    /**
     * A topology category.
     * <p>
     * For instance, the topology notifications.
     * <p>
     * Please note that this category was added to a later server version. Therefore, a compatible server version is
     * required to use it.
     *
     * @since 5.14
     */
    NotificationCategory TOPOLOGY = NotificationClassification.TOPOLOGY;

    /**
     * A generic category.
     * <p>
     * For instance, notifications that are not part of a more specific class.
     */
    NotificationCategory GENERIC = NotificationClassification.GENERIC;
}
