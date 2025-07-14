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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.GqlNotification;
import org.neo4j.driver.summary.InputPosition;

public final class InternalGqlNotification extends InternalGqlStatusObject implements GqlNotification {
    private final InputPosition position;
    private final NotificationSeverity severityLevel;
    private final String rawSeverityLevel;
    private final NotificationClassification classification;
    private final String rawClassification;

    // private Neo4j use only
    private final String code;
    private final String title;
    private final String description;

    public InternalGqlNotification(
            String gqlStatus,
            String statusDescription,
            Map<String, Value> diagnosticRecord,
            InputPosition position,
            NotificationSeverity severityLevel,
            String rawSeverityLevel,
            NotificationClassification classification,
            String rawClassification,
            String code,
            String title,
            String description) {
        super(gqlStatus, statusDescription, diagnosticRecord);
        this.position = position;
        this.severityLevel = severityLevel;
        this.rawSeverityLevel = rawSeverityLevel;
        this.classification = classification;
        this.rawClassification = rawClassification;
        this.code = code;
        this.title = title;
        this.description = description;
    }

    @Override
    public Optional<InputPosition> position() {
        return Optional.ofNullable(position);
    }

    @Override
    public Optional<NotificationSeverity> severity() {
        return Optional.ofNullable(severityLevel);
    }

    @Override
    public Optional<String> rawSeverity() {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (InternalGqlNotification) o;
        return Objects.equals(gqlStatus, that.gqlStatus)
                && Objects.equals(statusDescription, that.statusDescription)
                && Objects.equals(diagnosticRecord, that.diagnosticRecord)
                && Objects.equals(code, that.code)
                && Objects.equals(title, that.title)
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gqlStatus, statusDescription, diagnosticRecord, code, title, description);
    }

    @Override
    public String toString() {
        return "InternalGqlNotification{" + "gqlStatus='"
                + gqlStatus + '\'' + ", statusDescription='"
                + statusDescription + '\'' + ", diagnosticRecord="
                + diagnosticRecord + '\'' + ", code='"
                + code + '\'' + ", title='"
                + title + '\'' + ", description='"
                + description + '}';
    }

    // private Neo4j use only
    public String code() {
        return code;
    }

    // private Neo4j use only
    public String title() {
        return title;
    }

    // private Neo4j use only
    public String description() {
        return description;
    }
}
