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
package org.neo4j.driver.internal.util;

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.teeing;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.neo4j.driver.internal.summary.InternalDatabaseInfo.DEFAULT_DATABASE_INFO;
import static org.neo4j.driver.internal.value.NullValue.NULL;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.internal.InternalNotificationSeverity;
import org.neo4j.driver.internal.bolt.api.BoltConnection;
import org.neo4j.driver.internal.summary.InternalDatabaseInfo;
import org.neo4j.driver.internal.summary.InternalGqlStatusObject;
import org.neo4j.driver.internal.summary.InternalInputPosition;
import org.neo4j.driver.internal.summary.InternalNotification;
import org.neo4j.driver.internal.summary.InternalPlan;
import org.neo4j.driver.internal.summary.InternalProfiledPlan;
import org.neo4j.driver.internal.summary.InternalResultSummary;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.summary.DatabaseInfo;
import org.neo4j.driver.summary.GqlStatusObject;
import org.neo4j.driver.summary.InputPosition;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.ServerInfo;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.TypeSystem;

public class MetadataExtractor {
    public static final int ABSENT_QUERY_ID = -1;
    private static final String UNEXPECTED_TYPE_MSG_FMT = "Unexpected query type '%s', consider updating the driver";
    private static final Function<String, ProtocolException> UNEXPECTED_TYPE_EXCEPTION_SUPPLIER =
            (type) -> new ProtocolException(String.format(UNEXPECTED_TYPE_MSG_FMT, type));
    private static final Comparator<GqlStatusObject> GQL_STATUS_OBJECT_COMPARATOR =
            Comparator.comparingInt(gqlStatusObject -> {
                var status = gqlStatusObject.gqlStatus();
                if (status.startsWith("02")) {
                    return 0;
                } else if (status.startsWith("01")) {
                    return 1;
                } else if (status.startsWith("00")) {
                    return 2;
                } else if (status.startsWith("03")) {
                    return 3;
                } else {
                    return 4;
                }
            });
    private final String resultConsumedAfterMetadataKey;

    public MetadataExtractor(String resultConsumedAfterMetadataKey) {
        this.resultConsumedAfterMetadataKey = resultConsumedAfterMetadataKey;
    }

    public ResultSummary extractSummary(
            Query query,
            BoltConnection connection,
            long resultAvailableAfter,
            Map<String, Value> metadata,
            boolean legacyNotifications,
            GqlStatusObject gqlStatusObject) {
        ServerInfo serverInfo = new InternalServerInfo(
                connection.serverAgent(), connection.serverAddress(), connection.protocolVersion());
        var dbInfo = extractDatabaseInfo(metadata);
        Set<GqlStatusObject> gqlStatusObjects;
        List<Notification> notifications;
        if (legacyNotifications) {
            var gqlStatusObjectsAndNotifications = extractGqlStatusObjectsFromNotifications(metadata)
                    .collect(teeing(
                            collectingAndThen(
                                    toCollection(
                                            () -> (Set<GqlStatusObject>) new TreeSet<>(GQL_STATUS_OBJECT_COMPARATOR)),
                                    set -> {
                                        if (gqlStatusObject != null) {
                                            set.add(gqlStatusObject);
                                        }
                                        return unmodifiableSet(set);
                                    }),
                            toUnmodifiableList(),
                            GqlStatusObjectsAndNotifications::new));
            gqlStatusObjects = gqlStatusObjectsAndNotifications.gqlStatusObjects();
            notifications = gqlStatusObjectsAndNotifications.notifications();
        } else {
            gqlStatusObjects = extractGqlStatusObjects(metadata)
                    .collect(collectingAndThen(
                            toCollection(() -> (Set<GqlStatusObject>) new LinkedHashSet<GqlStatusObject>()),
                            Collections::unmodifiableSet));
            notifications = gqlStatusObjects.stream()
                    .flatMap(status -> status instanceof Notification ? Stream.of((Notification) status) : null)
                    .toList();
        }
        return new InternalResultSummary(
                query,
                serverInfo,
                dbInfo,
                extractQueryType(metadata),
                extractCounters(metadata),
                extractPlan(metadata),
                extractProfiledPlan(metadata),
                notifications,
                gqlStatusObjects,
                resultAvailableAfter,
                extractResultConsumedAfter(metadata, resultConsumedAfterMetadataKey));
    }

    static DatabaseInfo extractDatabaseInfo(Map<String, Value> metadata) {
        var dbValue = metadata.get("db");
        if (dbValue == null || dbValue.isNull()) {
            return DEFAULT_DATABASE_INFO;
        } else {
            return new InternalDatabaseInfo(dbValue.asString());
        }
    }

    private static QueryType extractQueryType(Map<String, Value> metadata) {
        var typeValue = metadata.get("type");
        if (typeValue != null) {
            return QueryType.fromCode(typeValue.asString(), UNEXPECTED_TYPE_EXCEPTION_SUPPLIER);
        }
        return null;
    }

    private static InternalSummaryCounters extractCounters(Map<String, Value> metadata) {
        var countersValue = metadata.get("stats");
        if (countersValue != null) {
            return new InternalSummaryCounters(
                    counterValue(countersValue, "nodes-created"),
                    counterValue(countersValue, "nodes-deleted"),
                    counterValue(countersValue, "relationships-created"),
                    counterValue(countersValue, "relationships-deleted"),
                    counterValue(countersValue, "properties-set"),
                    counterValue(countersValue, "labels-added"),
                    counterValue(countersValue, "labels-removed"),
                    counterValue(countersValue, "indexes-added"),
                    counterValue(countersValue, "indexes-removed"),
                    counterValue(countersValue, "constraints-added"),
                    counterValue(countersValue, "constraints-removed"),
                    counterValue(countersValue, "system-updates"));
        }
        return null;
    }

    private static int counterValue(Value countersValue, String name) {
        var value = countersValue.get(name);
        return value.isNull() ? 0 : value.asInt();
    }

    private static Plan extractPlan(Map<String, Value> metadata) {
        var planValue = metadata.get("plan");
        if (planValue != null) {
            return InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply(planValue);
        }
        return null;
    }

    private static ProfiledPlan extractProfiledPlan(Map<String, Value> metadata) {
        var profiledPlanValue = metadata.get("profile");
        if (profiledPlanValue != null) {
            return InternalProfiledPlan.PROFILED_PLAN_FROM_VALUE.apply(profiledPlanValue);
        }
        return null;
    }

    private static Stream<Notification> extractGqlStatusObjectsFromNotifications(Map<String, Value> metadata) {
        var notificationsValue = metadata.get("notifications");
        if (notificationsValue != null && TypeSystem.getDefault().LIST().isTypeOf(notificationsValue)) {
            var iterable = notificationsValue.values(value -> {
                var code = value.get("code").asString();
                var title = value.get("title").asString();
                var description = value.get("description").asString();
                var rawSeverityLevel =
                        value.containsKey("severity") ? value.get("severity").asString() : null;
                var severityLevel =
                        InternalNotificationSeverity.valueOf(rawSeverityLevel).orElse(null);
                var rawCategory =
                        value.containsKey("category") ? value.get("category").asString() : null;
                var category = InternalNotification.valueOf(rawCategory).orElse(null);

                var posValue = value.get("position");
                InputPosition position = null;
                if (posValue != NULL) {
                    position = new InternalInputPosition(
                            posValue.get("offset").asInt(),
                            posValue.get("line").asInt(),
                            posValue.get("column").asInt());
                }

                var gqlStatusCode = "03N42";
                var gqlStatusDescription = description;
                if (NotificationSeverity.WARNING.equals(severityLevel)) {
                    gqlStatusCode = "01N42";
                    if (gqlStatusDescription == null || "null".equals(gqlStatusDescription)) {
                        gqlStatusDescription = "warn: unknown warning";
                    }
                } else {
                    if (gqlStatusDescription == null || "null".equals(gqlStatusDescription)) {
                        gqlStatusDescription = "info: unknown notification";
                    }
                }

                var diagnosticRecord = new HashMap<String, Value>(3);
                diagnosticRecord.put("OPERATION", Values.value(""));
                diagnosticRecord.put("OPERATION_CODE", Values.value("0"));
                diagnosticRecord.put("CURRENT_SCHEMA", Values.value("/"));
                if (rawSeverityLevel != null) {
                    diagnosticRecord.put("_severity", Values.value(rawSeverityLevel));
                }
                if (rawCategory != null) {
                    diagnosticRecord.put("_classification", Values.value(rawCategory));
                }
                if (position != null) {
                    diagnosticRecord.put(
                            "_position",
                            Values.value(Map.of(
                                    "offset",
                                    Values.value(position.offset()),
                                    "line",
                                    Values.value(position.line()),
                                    "column",
                                    Values.value(position.column()))));
                }

                return new InternalNotification(
                        gqlStatusCode,
                        gqlStatusDescription,
                        Collections.unmodifiableMap(diagnosticRecord),
                        code,
                        title,
                        description,
                        severityLevel,
                        rawSeverityLevel,
                        (NotificationClassification) category,
                        rawCategory,
                        position);
            });
            return StreamSupport.stream(iterable.spliterator(), false).map(Notification.class::cast);
        } else {
            return Stream.empty();
        }
    }

    private static Stream<GqlStatusObject> extractGqlStatusObjects(Map<String, Value> metadata) {
        var statuses = metadata.get("statuses");
        if (statuses != null && TypeSystem.getDefault().LIST().isTypeOf(statuses)) {
            var iterable = statuses.values(MetadataExtractor::extractGqlStatusObject);
            return StreamSupport.stream(iterable.spliterator(), false);
        } else {
            return Stream.empty();
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private static GqlStatusObject extractGqlStatusObject(Value value) {
        var status = value.get("gql_status").asString();
        var description = value.get("status_description").asString();
        Map<String, Value> diagnosticRecord;
        var diagnosticRecordValue = value.get("diagnostic_record");
        if (diagnosticRecordValue != null && TypeSystem.getDefault().MAP().isTypeOf(diagnosticRecordValue)) {
            var containsOperation = diagnosticRecordValue.containsKey("OPERATION");
            var containsOperationCode = diagnosticRecordValue.containsKey("OPERATION_CODE");
            var containsCurrentSchema = diagnosticRecordValue.containsKey("CURRENT_SCHEMA");
            if (containsOperation && containsOperationCode && containsCurrentSchema) {
                diagnosticRecord = diagnosticRecordValue.asMap(Values::value);
            } else {
                diagnosticRecord = new HashMap<>(diagnosticRecordValue.asMap(Values::value));
                if (!containsOperation) {
                    diagnosticRecord.put("OPERATION", Values.value(""));
                }
                if (!containsOperationCode) {
                    diagnosticRecord.put("OPERATION_CODE", Values.value("0"));
                }
                if (!containsCurrentSchema) {
                    diagnosticRecord.put("CURRENT_SCHEMA", Values.value("/"));
                }
                diagnosticRecord = Collections.unmodifiableMap(diagnosticRecord);
            }
        } else {
            diagnosticRecord = Map.ofEntries(
                    Map.entry("OPERATION", Values.value("")),
                    Map.entry("OPERATION_CODE", Values.value("0")),
                    Map.entry("CURRENT_SCHEMA", Values.value("/")));
        }

        var neo4jCode = value.get("neo4j_code").asString(null);

        if (neo4jCode == null || neo4jCode.trim().isEmpty()) {
            return new InternalGqlStatusObject(status, description, diagnosticRecord);
        } else {
            var title = value.get("title").asString();
            var notificationDescription =
                    value.containsKey("description") ? value.get("description").asString() : description;

            var positionValue = diagnosticRecord.get("_position");
            InputPosition position = null;
            if (positionValue != null && TypeSystem.getDefault().MAP().isTypeOf(positionValue)) {
                var offset = getAsInt(positionValue, "offset");
                var line = getAsInt(positionValue, "line");
                var column = getAsInt(positionValue, "column");
                if (Stream.of(offset, line, column).allMatch(OptionalInt::isPresent)) {
                    position = new InternalInputPosition(offset.getAsInt(), line.getAsInt(), column.getAsInt());
                }
            }

            var severityValue = diagnosticRecord.get("_severity");
            String rawSeverity = null;
            if (severityValue != null && TypeSystem.getDefault().STRING().isTypeOf(severityValue)) {
                rawSeverity = severityValue.asString();
            }
            var severity = InternalNotificationSeverity.valueOf(rawSeverity).orElse(null);

            var classificationValue = diagnosticRecord.get("_classification");
            String rawClassification = null;
            if (classificationValue != null && TypeSystem.getDefault().STRING().isTypeOf(classificationValue)) {
                rawClassification = classificationValue.asString();
            }
            var classification = (NotificationClassification)
                    InternalNotification.valueOf(rawClassification).orElse(null);

            return new InternalNotification(
                    status,
                    description,
                    diagnosticRecord,
                    neo4jCode,
                    title,
                    notificationDescription,
                    severity,
                    rawSeverity,
                    classification,
                    rawClassification,
                    position);
        }
    }

    private static OptionalInt getAsInt(MapAccessor mapAccessor, String key) {
        var value = mapAccessor.get(key);
        if (value != null && TypeSystem.getDefault().INTEGER().isTypeOf(value)) {
            return OptionalInt.of(value.asInt());
        } else {
            return OptionalInt.empty();
        }
    }

    private static long extractResultConsumedAfter(Map<String, Value> metadata, String key) {
        var resultConsumedAfterValue = metadata.get(key);
        if (resultConsumedAfterValue != null) {
            return resultConsumedAfterValue.asLong();
        }
        return -1;
    }

    private record GqlStatusObjectsAndNotifications(
            Set<GqlStatusObject> gqlStatusObjects, List<Notification> notifications) {}
}
