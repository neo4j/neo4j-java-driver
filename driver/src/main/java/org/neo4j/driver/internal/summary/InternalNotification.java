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

import static org.neo4j.driver.internal.value.NullValue.NULL;

import java.util.Optional;
import java.util.function.Function;
import org.neo4j.driver.NotificationCategory;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalNotificationCategory;
import org.neo4j.driver.internal.InternalNotificationSeverity;
import org.neo4j.driver.summary.InputPosition;
import org.neo4j.driver.summary.Notification;

public class InternalNotification implements Notification {
    public static final Function<Value, Notification> VALUE_TO_NOTIFICATION = value -> {
        var code = value.get("code").asString();
        var title = value.get("title").asString();
        var description = value.get("description").asString();
        var rawSeverityLevel =
                value.containsKey("severity") ? value.get("severity").asString() : null;
        var severityLevel =
                InternalNotificationSeverity.valueOf(rawSeverityLevel).orElse(null);
        var rawCategory = value.containsKey("category") ? value.get("category").asString() : null;
        var category = InternalNotificationCategory.valueOf(rawCategory).orElse(null);

        var posValue = value.get("position");
        InputPosition position = null;
        if (posValue != NULL) {
            position = new InternalInputPosition(
                    posValue.get("offset").asInt(),
                    posValue.get("line").asInt(),
                    posValue.get("column").asInt());
        }

        return new InternalNotification(
                code, title, description, severityLevel, rawSeverityLevel, category, rawCategory, position);
    };

    private final String code;
    private final String title;
    private final String description;
    private final NotificationSeverity severityLevel;
    private final String rawSeverityLevel;
    private final NotificationCategory category;
    private final String rawCategory;
    private final InputPosition position;

    public InternalNotification(
            String code,
            String title,
            String description,
            NotificationSeverity severityLevel,
            String rawSeverityLevel,
            NotificationCategory category,
            String rawCategory,
            InputPosition position) {
        this.code = code;
        this.title = title;
        this.description = description;
        this.severityLevel = severityLevel;
        this.rawSeverityLevel = rawSeverityLevel;
        this.category = category;
        this.rawCategory = rawCategory;
        this.position = position;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public String title() {
        return title;
    }

    @Override
    public String description() {
        return description;
    }

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
    public Optional<NotificationCategory> category() {
        return Optional.ofNullable(category);
    }

    @Override
    public Optional<String> rawCategory() {
        return Optional.ofNullable(rawCategory);
    }

    @Override
    public String toString() {
        var info = "code=" + code + ", title=" + title + ", description=" + description + ", severityLevel="
                + severityLevel + ", rawSeverityLevel=" + rawSeverityLevel + ", category=" + category + ", rawCategory="
                + rawCategory;
        return position == null ? info : info + ", position={" + position + "}";
    }
}
