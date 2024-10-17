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

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.driver.NotificationSeverity;

public record InternalNotificationSeverity(Type type, int level) implements NotificationSeverity {
    public InternalNotificationSeverity {
        Objects.requireNonNull(type, "type must not be null");
    }

    @Override
    public int compareTo(NotificationSeverity severity) {
        return Integer.compare(level, ((InternalNotificationSeverity) severity).level());
    }

    public enum Type {
        INFORMATION,
        WARNING,
        OFF
    }

    @SuppressWarnings("DuplicatedCode")
    public static Optional<NotificationSeverity> valueOf(String value) {
        return Arrays.stream(Type.values())
                .filter(type -> type.toString().equals(value))
                .findFirst()
                .map(type -> switch (type) {
                    case INFORMATION -> NotificationSeverity.INFORMATION;
                    case WARNING -> NotificationSeverity.WARNING;
                    case OFF -> NotificationSeverity.OFF;
                });
    }
}
