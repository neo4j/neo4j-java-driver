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
package org.neo4j.driver.summary;

import java.util.Optional;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.driver.internal.summary.InternalGqlNotification;

/**
 * A notification subtype of the {@link GqlStatusObject}.
 *
 * @see GqlStatusObject
 * @since 5.28.8
 */
public sealed interface GqlNotification extends GqlStatusObject permits InternalGqlNotification {

    /**
     * Returns a position in the query where this notification points to.
     * <p>
     * Not all notifications have a unique position to point to and in that case an empty {@link Optional} is returned.
     *
     * @return an {@link Optional} of the {@link InputPosition} if available or an empty {@link Optional} otherwise
     */
    Optional<InputPosition> position();

    /**
     * Returns the severity level of the notification derived from the diagnostic record.
     *
     * @return the severity level of the notification
     * @see #diagnosticRecord()
     */
    Optional<NotificationSeverity> severity();

    /**
     * Returns the raw severity level of the notification as a String value retrieved directly from the diagnostic
     * record.
     *
     * @return the severity level of the notification
     * @see #diagnosticRecord()
     */
    Optional<String> rawSeverity();

    /**
     * Returns {@link NotificationClassification} derived from the diagnostic record.
     *
     * @return an {@link Optional} of {@link NotificationClassification} or an empty {@link Optional} when the
     * classification is either absent or unrecognised
     * @see #diagnosticRecord()
     */
    Optional<NotificationClassification> classification();

    /**
     * Returns notification classification from the diagnostic record as a {@link String} value retrieved directly from
     * the diagnostic record.
     *
     * @return an {@link Optional} of notification classification or an empty {@link Optional} when it is absent
     * @see #diagnosticRecord()
     */
    Optional<String> rawClassification();
}
