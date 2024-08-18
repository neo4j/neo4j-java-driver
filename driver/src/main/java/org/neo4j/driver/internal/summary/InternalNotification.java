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
package org.neo4j.driver.internal.summary;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.NotificationCategory;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.InputPosition;
import org.neo4j.driver.summary.Notification;

public class InternalNotification extends InternalGqlStatusObject implements Notification {
    public static Optional<NotificationCategory> valueOf(String value) {
        return Arrays.stream(NotificationClassification.values())
                .filter(type -> type.toString().equals(value))
                .findFirst()
                .map(type -> switch (type) {
                    case HINT -> NotificationCategory.HINT;
                    case UNRECOGNIZED -> NotificationCategory.UNRECOGNIZED;
                    case UNSUPPORTED -> NotificationCategory.UNSUPPORTED;
                    case PERFORMANCE -> NotificationCategory.PERFORMANCE;
                    case DEPRECATION -> NotificationCategory.DEPRECATION;
                    case SECURITY -> NotificationCategory.SECURITY;
                    case TOPOLOGY -> NotificationCategory.TOPOLOGY;
                    case GENERIC -> NotificationCategory.GENERIC;
                    case SCHEMA -> NotificationCategory.SCHEMA;
                });
    }

    private final String code;
    private final String title;
    private final String description;
    private final NotificationSeverity severityLevel;
    private final String rawSeverityLevel;
    private final NotificationClassification classification;
    private final String rawClassification;
    private final InputPosition position;

    public InternalNotification(
            String gqlStatus,
            String statusDescription,
            Map<String, Value> diagnosticRecord,
            String code,
            String title,
            String description,
            NotificationSeverity severityLevel,
            String rawSeverityLevel,
            NotificationClassification classification,
            String rawClassification,
            InputPosition position) {
        super(gqlStatus, statusDescription, diagnosticRecord);
        this.code = Objects.requireNonNull(code);
        this.title = title;
        this.description = description;
        this.severityLevel = severityLevel;
        this.rawSeverityLevel = rawSeverityLevel;
        this.classification = classification;
        this.rawClassification = rawClassification;
        this.position = position;
    }

    @SuppressWarnings({"deprecation", "RedundantSuppression"})
    @Override
    public String code() {
        return code;
    }

    @SuppressWarnings({"deprecation", "RedundantSuppression"})
    @Override
    public String title() {
        return title;
    }

    @SuppressWarnings({"deprecation", "RedundantSuppression"})
    @Override
    public String description() {
        return description;
    }

    @SuppressWarnings({"deprecation", "RedundantSuppression"})
    @Override
    public InputPosition position() {
        return position;
    }

    @Override
    public Optional<NotificationSeverity> severityLevel() {
        return Optional.ofNullable(severityLevel);
    }

    @Override
    public Optional<String> rawSeverityLevel() {
        return Optional.ofNullable(rawSeverityLevel);
    }

    @Override
    public Optional<NotificationClassification> classification() {
        return Optional.ofNullable(classification);
    }

    @Override
    public Optional<String> rawClassification() {
        return Optional.ofNullable(rawClassification);
    }

    @Override
    public Optional<NotificationCategory> category() {
        return Optional.ofNullable(classification);
    }

    @Override
    public Optional<String> rawCategory() {
        return Optional.ofNullable(rawClassification);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        var that = (InternalNotification) o;
        return Objects.equals(code, that.code)
                && Objects.equals(title, that.title)
                && Objects.equals(description, that.description)
                && Objects.equals(severityLevel, that.severityLevel)
                && Objects.equals(rawSeverityLevel, that.rawSeverityLevel)
                && classification == that.classification
                && Objects.equals(rawClassification, that.rawClassification)
                && Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                code,
                title,
                description,
                severityLevel,
                rawSeverityLevel,
                classification,
                rawClassification,
                position);
    }

    @Override
    public String toString() {
        var info = "code=" + code + ", title=" + title + ", description=" + description + ", severityLevel="
                + severityLevel + ", rawSeverityLevel=" + rawSeverityLevel + ", classification=" + classification
                + ", rawClassification="
                + rawClassification;
        return position == null ? info : info + ", position={" + position + "}";
    }
}
