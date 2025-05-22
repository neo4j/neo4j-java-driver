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
package neo4j.org.testkit.backend.messages.requests;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import neo4j.org.testkit.backend.messages.responses.Summary;
import org.neo4j.driver.internal.InternalNotificationSeverity;
import org.neo4j.driver.summary.InputPosition;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.QueryType;

public class SummaryUtil {
    @SuppressWarnings("deprecation")
    public static Summary.SummaryBody toSummaryBody(org.neo4j.driver.summary.ResultSummary summary) {
        var serverInfo = Summary.ServerInfo.builder()
                .address(summary.server().address())
                .protocolVersion(summary.server().protocolVersion())
                .agent(summary.server().agent())
                .build();
        var summaryCounters = summary.counters();
        var counters = Summary.Counters.builder()
                .constraintsAdded(summaryCounters.constraintsAdded())
                .constraintsRemoved(summaryCounters.constraintsRemoved())
                .containsSystemUpdates(summaryCounters.containsSystemUpdates())
                .containsUpdates(summaryCounters.containsUpdates())
                .indexesAdded(summaryCounters.indexesAdded())
                .indexesRemoved(summaryCounters.indexesRemoved())
                .labelsAdded(summaryCounters.labelsAdded())
                .labelsRemoved(summaryCounters.labelsRemoved())
                .nodesCreated(summaryCounters.nodesCreated())
                .nodesDeleted(summaryCounters.nodesDeleted())
                .propertiesSet(summaryCounters.propertiesSet())
                .relationshipsCreated(summaryCounters.relationshipsCreated())
                .relationshipsDeleted(summaryCounters.relationshipsDeleted())
                .systemUpdates(summaryCounters.systemUpdates())
                .build();
        var summaryQuery = summary.query();
        var query = Summary.Query.builder()
                .text(summaryQuery.text())
                .parameters(summaryQuery.parameters().asMap(Function.identity(), null))
                .build();
        var notifications = summary.notifications().stream()
                .map(s -> Summary.Notification.builder()
                        .code(s.code())
                        .title(s.title())
                        .description(s.description())
                        .position(toInputPosition(s.position()))
                        .severityLevel(s.severityLevel()
                                .map(InternalNotificationSeverity.class::cast)
                                .map(InternalNotificationSeverity::type)
                                .map(InternalNotificationSeverity.Type::toString)
                                .orElse("UNKNOWN"))
                        .rawSeverityLevel(s.rawSeverityLevel().orElse(null))
                        .category(s.category().map(Objects::toString).orElse("UNKNOWN"))
                        .rawCategory(s.rawCategory().orElse(null))
                        .build())
                .collect(Collectors.toList());
        var gqlStatusObjects = summary.gqlStatusObjects().stream()
                .map(gqlStatusObject -> {
                    var builder = Summary.GqlStatusObject.builder()
                            .gqlStatus(gqlStatusObject.gqlStatus())
                            .statusDescription(gqlStatusObject.statusDescription())
                            .diagnosticRecord(gqlStatusObject.diagnosticRecord());
                    if (gqlStatusObject instanceof Notification notification) {
                        builder = builder.position(toInputPosition(notification.position()))
                                .severity(notification
                                        .severityLevel()
                                        .map(InternalNotificationSeverity.class::cast)
                                        .map(InternalNotificationSeverity::type)
                                        .map(InternalNotificationSeverity.Type::toString)
                                        .orElse("UNKNOWN"))
                                .rawSeverity(notification.rawSeverityLevel().orElse(null))
                                .classification(notification
                                        .classification()
                                        .map(Objects::toString)
                                        .orElse("UNKNOWN"))
                                .rawClassification(
                                        notification.rawClassification().orElse(null))
                                .notification(true);
                    } else {
                        builder = builder.position(null)
                                .severity("UNKNOWN")
                                .classification("UNKNOWN")
                                .diagnosticRecord(gqlStatusObject.diagnosticRecord());
                    }
                    return builder.build();
                })
                .toList();
        return Summary.SummaryBody.builder()
                .serverInfo(serverInfo)
                .counters(counters)
                .query(query)
                .database(summary.database().name())
                .notifications(notifications)
                .gqlStatusObjects(gqlStatusObjects)
                .plan(toPlan(summary.plan()))
                .profile(toProfile(summary.profile()))
                .queryType(toQueryType(summary.queryType()))
                .resultAvailableAfter(
                        summary.resultAvailableAfter(TimeUnit.MILLISECONDS) == -1
                                ? null
                                : summary.resultAvailableAfter(TimeUnit.MILLISECONDS))
                .resultConsumedAfter(
                        summary.resultConsumedAfter(TimeUnit.MILLISECONDS) == -1
                                ? null
                                : summary.resultConsumedAfter(TimeUnit.MILLISECONDS))
                .build();
    }

    private static Summary.InputPosition toInputPosition(InputPosition position) {
        if (position == null) {
            return null;
        }
        return Summary.InputPosition.builder()
                .offset(position.offset())
                .line(position.line())
                .column(position.column())
                .build();
    }

    private static Summary.Plan toPlan(Plan plan) {
        if (plan == null) {
            return null;
        }
        Map<String, Object> args = new HashMap<>();
        plan.arguments().forEach((key, value) -> args.put(key, value.asObject()));
        return Summary.Plan.builder()
                .operatorType(plan.operatorType())
                .args(args)
                .identifiers(plan.identifiers())
                .children(plan.children().stream().map(SummaryUtil::toPlan).collect(Collectors.toList()))
                .build();
    }

    private static Summary.Profile toProfile(ProfiledPlan plan) {
        if (plan == null) {
            return null;
        }
        Map<String, Object> args = new HashMap<>();
        plan.arguments().forEach((key, value) -> args.put(key, value.asObject()));
        return Summary.Profile.builder()
                .operatorType(plan.operatorType())
                .args(args)
                .identifiers(plan.identifiers())
                .dbHits(plan.dbHits())
                .rows(plan.records())
                .hasPageCacheStats(plan.hasPageCacheStats())
                .pageCacheHits(plan.pageCacheHits())
                .pageCacheMisses(plan.pageCacheMisses())
                .pageCacheHitRatio(plan.pageCacheHitRatio())
                .time(plan.time())
                .children(plan.children().stream().map(SummaryUtil::toProfile).collect(Collectors.toList()))
                .build();
    }

    private static String toQueryType(QueryType type) {
        if (type == null) {
            return null;
        }

        String typeStr;
        if (type == QueryType.READ_ONLY) {
            typeStr = "r";
        } else if (type == QueryType.READ_WRITE) {
            typeStr = "rw";
        } else if (type == QueryType.WRITE_ONLY) {
            typeStr = "w";
        } else if (type == QueryType.SCHEMA_WRITE) {
            typeStr = "s";
        } else {
            throw new IllegalStateException("Unexpected query type");
        }
        return typeStr;
    }
}
