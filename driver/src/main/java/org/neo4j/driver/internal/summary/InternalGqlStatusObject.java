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
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.summary.GqlStatusObject;

public class InternalGqlStatusObject implements GqlStatusObject {
    public static final GqlStatusObject SUCCESS = new InternalGqlStatusObject(
            "00000",
            "note: successful completion",
            Map.ofEntries(
                    Map.entry("CURRENT_SCHEMA", Values.value("/")),
                    Map.entry("OPERATION", Values.value("")),
                    Map.entry("OPERATION_CODE", Values.value("0"))));
    public static final GqlStatusObject NO_DATA = new InternalGqlStatusObject(
            "02000",
            "note: no data",
            Map.ofEntries(
                    Map.entry("CURRENT_SCHEMA", Values.value("/")),
                    Map.entry("OPERATION", Values.value("")),
                    Map.entry("OPERATION_CODE", Values.value("0"))));
    public static final GqlStatusObject NO_DATA_UNKNOWN = new InternalGqlStatusObject(
            "02N42",
            "note: no data - unknown subcondition",
            Map.ofEntries(
                    Map.entry("CURRENT_SCHEMA", Values.value("/")),
                    Map.entry("OPERATION", Values.value("")),
                    Map.entry("OPERATION_CODE", Values.value("0"))));
    public static final GqlStatusObject OMITTED_RESULT = new InternalGqlStatusObject(
            "00001",
            "note: successful completion - omitted result",
            Map.ofEntries(
                    Map.entry("CURRENT_SCHEMA", Values.value("/")),
                    Map.entry("OPERATION", Values.value("")),
                    Map.entry("OPERATION_CODE", Values.value("0"))));

    private final String gqlStatus;
    private final String statusDescription;
    private final Map<String, Value> diagnosticRecord;

    public InternalGqlStatusObject(String gqlStatus, String statusDescription, Map<String, Value> diagnosticRecord) {
        this.gqlStatus = Objects.requireNonNull(gqlStatus);
        this.statusDescription = Objects.requireNonNull(statusDescription);
        this.diagnosticRecord = Objects.requireNonNull(diagnosticRecord);
    }

    @Override
    public String gqlStatus() {
        return gqlStatus;
    }

    @Override
    public String statusDescription() {
        return statusDescription;
    }

    @Override
    public Map<String, Value> diagnosticRecord() {
        return diagnosticRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (InternalGqlStatusObject) o;
        return Objects.equals(gqlStatus, that.gqlStatus)
                && Objects.equals(statusDescription, that.statusDescription)
                && Objects.equals(diagnosticRecord, that.diagnosticRecord);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gqlStatus, statusDescription, diagnosticRecord);
    }

    @Override
    public String toString() {
        return "InternalGqlStatusObject{" + "gqlStatus='"
                + gqlStatus + '\'' + ", statusDescription='"
                + statusDescription + '\'' + ", diagnosticRecord="
                + diagnosticRecord + '}';
    }
}
