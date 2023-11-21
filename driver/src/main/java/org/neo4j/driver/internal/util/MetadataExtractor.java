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

import static org.neo4j.driver.internal.summary.InternalDatabaseInfo.DEFAULT_DATABASE_INFO;
import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.internal.DatabaseBookmark;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.summary.InternalDatabaseInfo;
import org.neo4j.driver.internal.summary.InternalNotification;
import org.neo4j.driver.internal.summary.InternalPlan;
import org.neo4j.driver.internal.summary.InternalProfiledPlan;
import org.neo4j.driver.internal.summary.InternalResultSummary;
import org.neo4j.driver.internal.summary.InternalServerInfo;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.summary.DatabaseInfo;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.ServerInfo;

public class MetadataExtractor {
    public static final int ABSENT_QUERY_ID = -1;
    private static final String UNEXPECTED_TYPE_MSG_FMT = "Unexpected query type '%s', consider updating the driver";
    private static final Function<String, ProtocolException> UNEXPECTED_TYPE_EXCEPTION_SUPPLIER =
            (type) -> new ProtocolException(String.format(UNEXPECTED_TYPE_MSG_FMT, type));
    private final String resultAvailableAfterMetadataKey;
    private final String resultConsumedAfterMetadataKey;

    public MetadataExtractor(String resultAvailableAfterMetadataKey, String resultConsumedAfterMetadataKey) {
        this.resultAvailableAfterMetadataKey = resultAvailableAfterMetadataKey;
        this.resultConsumedAfterMetadataKey = resultConsumedAfterMetadataKey;
    }

    public QueryKeys extractQueryKeys(Map<String, Value> metadata) {
        var keysValue = metadata.get("fields");
        if (keysValue != null) {
            if (!keysValue.isEmpty()) {
                var keys = new QueryKeys(keysValue.size());
                for (var value : keysValue.values()) {
                    keys.add(value.asString());
                }

                return keys;
            }
        }
        return QueryKeys.empty();
    }

    public long extractQueryId(Map<String, Value> metadata) {
        var queryId = metadata.get("qid");
        if (queryId != null) {
            return queryId.asLong();
        }
        return ABSENT_QUERY_ID;
    }

    public long extractResultAvailableAfter(Map<String, Value> metadata) {
        var resultAvailableAfterValue = metadata.get(resultAvailableAfterMetadataKey);
        if (resultAvailableAfterValue != null) {
            return resultAvailableAfterValue.asLong();
        }
        return -1;
    }

    public ResultSummary extractSummary(
            Query query, Connection connection, long resultAvailableAfter, Map<String, Value> metadata) {
        ServerInfo serverInfo = new InternalServerInfo(
                connection.serverAgent(),
                connection.serverAddress(),
                connection.protocol().version());
        var dbInfo = extractDatabaseInfo(metadata);
        return new InternalResultSummary(
                query,
                serverInfo,
                dbInfo,
                extractQueryType(metadata),
                extractCounters(metadata),
                extractPlan(metadata),
                extractProfiledPlan(metadata),
                extractNotifications(metadata),
                resultAvailableAfter,
                extractResultConsumedAfter(metadata, resultConsumedAfterMetadataKey));
    }

    public static DatabaseBookmark extractDatabaseBookmark(Map<String, Value> metadata) {
        var databaseName = extractDatabaseInfo(metadata).name();
        var bookmark = extractBookmark(metadata);
        return new DatabaseBookmark(databaseName, bookmark);
    }

    public static Value extractServer(Map<String, Value> metadata) {
        var versionValue = metadata.get("server");
        if (versionValue == null || versionValue.isNull()) {
            throw new UntrustedServerException("Server provides no product identifier");
        }
        var serverAgent = versionValue.asString();
        if (!serverAgent.startsWith("Neo4j/")) {
            throw new UntrustedServerException(
                    "Server does not identify as a genuine Neo4j instance: '" + serverAgent + "'");
        }
        return versionValue;
    }

    static DatabaseInfo extractDatabaseInfo(Map<String, Value> metadata) {
        var dbValue = metadata.get("db");
        if (dbValue == null || dbValue.isNull()) {
            return DEFAULT_DATABASE_INFO;
        } else {
            return new InternalDatabaseInfo(dbValue.asString());
        }
    }

    static Bookmark extractBookmark(Map<String, Value> metadata) {
        var bookmarkValue = metadata.get("bookmark");
        Bookmark bookmark = null;
        if (bookmarkValue != null && !bookmarkValue.isNull() && bookmarkValue.hasType(TYPE_SYSTEM.STRING())) {
            bookmark = InternalBookmark.parse(bookmarkValue.asString());
        }
        return bookmark;
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

    private static List<Notification> extractNotifications(Map<String, Value> metadata) {
        var notificationsValue = metadata.get("notifications");
        if (notificationsValue != null) {
            return notificationsValue.asList(InternalNotification.VALUE_TO_NOTIFICATION);
        }
        return Collections.emptyList();
    }

    private static long extractResultConsumedAfter(Map<String, Value> metadata, String key) {
        var resultConsumedAfterValue = metadata.get(key);
        if (resultConsumedAfterValue != null) {
            return resultConsumedAfterValue.asLong();
        }
        return -1;
    }

    public static Set<String> extractBoltPatches(Map<String, Value> metadata) {
        var boltPatch = metadata.get("patch_bolt");
        if (boltPatch != null && !boltPatch.isNull()) {
            return new HashSet<>(boltPatch.asList(Value::asString));
        } else {
            return Collections.emptySet();
        }
    }
}
