/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.NullRecord;
import neo4j.org.testkit.backend.messages.responses.Summary;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.summary.Category;
import org.neo4j.driver.summary.InputPosition;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.Severity;
import org.neo4j.driver.summary.SummaryCounters;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class ResultConsume implements TestkitRequest {
    private ResultConsumeBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        try {
            Result result = testkitState.getResultHolder(data.getResultId()).getResult();
            return createResponse(result.consume());
        } catch (NoSuchRecordException ignored) {
            return NullRecord.builder().build();
        }
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncResultHolder(data.getResultId())
                .thenCompose(
                        resultCursorHolder -> resultCursorHolder.getResult().consumeAsync())
                .thenApply(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processRx(TestkitState testkitState) {
        return testkitState
                .getRxResultHolder(data.getResultId())
                .flatMap(
                        resultHolder -> Mono.fromDirect(resultHolder.getResult().consume()))
                .map(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState
                .getReactiveResultHolder(data.getResultId())
                .flatMap(resultHolder -> Mono.fromDirect(
                        flowPublisherToFlux(resultHolder.getResult().consume())))
                .map(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState
                .getReactiveResultStreamsHolder(data.getResultId())
                .flatMap(
                        resultHolder -> Mono.fromDirect(resultHolder.getResult().consume()))
                .map(this::createResponse);
    }

    private Summary createResponse(org.neo4j.driver.summary.ResultSummary summary) {
        Summary.ServerInfo serverInfo = Summary.ServerInfo.builder()
                .address(summary.server().address())
                .protocolVersion(summary.server().protocolVersion())
                .agent(summary.server().agent())
                .build();
        SummaryCounters summaryCounters = summary.counters();
        Summary.Counters counters = Summary.Counters.builder()
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
        Query summaryQuery = summary.query();
        Summary.Query query = Summary.Query.builder()
                .text(summaryQuery.text())
                .parameters(summaryQuery.parameters().asMap(Function.identity(), null))
                .build();
        @SuppressWarnings("deprecation")
        List<Summary.Notification> notifications = summary.notifications().stream()
                .map(s -> Summary.Notification.builder()
                        .code(s.code())
                        .title(s.title())
                        .description(s.description())
                        .position(toInputPosition(s.position()))
                        .severity(s.severity())
                        .severityLevel(s.severityLevel().map(Severity::name).orElse(null))
                        .rawSeverityLevel(s.rawSeverityLevel().orElse(null))
                        .category(s.category().map(Category::name).orElse(null))
                        .rawCategory(s.rawCategory().orElse(null))
                        .build())
                .collect(Collectors.toList());
        Summary.SummaryBody data = Summary.SummaryBody.builder()
                .serverInfo(serverInfo)
                .counters(counters)
                .query(query)
                .database(summary.database().name())
                .notifications(notifications)
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
        return Summary.builder().data(data).build();
    }

    @Setter
    @Getter
    public static class ResultConsumeBody {
        private String resultId;
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
                .children(plan.children().stream().map(ResultConsume::toPlan).collect(Collectors.toList()))
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
                .children(plan.children().stream().map(ResultConsume::toProfile).collect(Collectors.toList()))
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
