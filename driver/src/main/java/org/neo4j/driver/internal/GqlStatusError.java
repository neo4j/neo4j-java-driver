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

import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

public enum GqlStatusError {
    UNKNOWN("50N42", "general processing exception - unexpected error");

    public static final Map<String, Value> DIAGNOSTIC_RECORD = Map.ofEntries(
            Map.entry("OPERATION", Values.value("")),
            Map.entry("OPERATION_CODE", Values.value("0")),
            Map.entry("CURRENT_SCHEMA", Values.value("/")));

    private final String status;
    private final String explanation;

    @SuppressWarnings("SameParameterValue")
    GqlStatusError(String status, String explanation) {
        this.status = status;
        this.explanation = explanation;
    }

    public String getStatus() {
        return status;
    }

    public String getStatusDescription(String message) {
        return String.format("error: %s. %s", explanation, message);
    }
}
