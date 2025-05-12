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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.RunTest;
import neo4j.org.testkit.backend.messages.responses.SkipTest;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class StartSubTest implements TestkitRequest {
    interface SkipDeciderInterface {
        SkipDecision check(Map<String, Object> params);
    }

    public record SkipDecision(boolean skipped, String reason) {

        static SkipDecision ofNonSkipped() {
            return new SkipDecision(false, null);
        }

        static SkipDecision ofSkipped(String reason) {
            return new SkipDecision(true, reason);
        }
    }

    private static final Map<String, SkipDeciderInterface> COMMON_SKIP_PATTERN_TO_CHECK = new HashMap<>();
    private static final Map<String, SkipDeciderInterface> ASYNC_SKIP_PATTERN_TO_CHECK = new HashMap<>();
    private static final Map<String, SkipDeciderInterface> REACTIVE_LEGACY_SKIP_PATTERN_TO_CHECK = new HashMap<>();
    private static final Map<String, SkipDeciderInterface> REACTIVE_SKIP_PATTERN_TO_CHECK = new HashMap<>();

    private static SkipDecision checkTzIdSupported(Map<String, Object> params) {
        var tzId = (String) params.get("tz_id");
        return ZoneId.getAvailableZoneIds().contains(tzId)
                ? SkipDecision.ofNonSkipped()
                : SkipDecision.ofSkipped("Timezone not supported: " + tzId);
    }

    private static SkipDecision checkDateTimeSupported(Map<String, Object> params) {
        @SuppressWarnings("unchecked")
        var dt_param = (HashMap<String, Object>) params.get("dt");
        if (dt_param == null) {
            throw new RuntimeException("params expected to contain 'dt'");
        }
        @SuppressWarnings("unchecked")
        var data = (HashMap<String, Object>) dt_param.get("data");
        if (data == null) {
            throw new RuntimeException("param 'dt' expected to contain 'data'");
        }
        var year = (Integer) data.get("year");
        var month = (Integer) data.get("month");
        var day = (Integer) data.get("day");
        var hour = (Integer) data.get("hour");
        var minute = (Integer) data.get("minute");
        var second = (Integer) data.get("second");
        var nano = (Integer) data.get("nanosecond");
        var utcOffset = (Integer) data.get("utc_offset_s");
        var tzId = (String) data.get("timezone_id");
        try {
            var dt = ZonedDateTime.of(year, month, day, hour, minute, second, nano, ZoneId.of(tzId));
            if (dt.getOffset().getTotalSeconds() != utcOffset) {
                throw new DateTimeException(String.format(
                        "Unmatched UTC offset. TestKit expected %d, local zone db yielded %d",
                        utcOffset, dt.getOffset().getTotalSeconds()));
            }
            return SkipDecision.ofNonSkipped();
        } catch (DateTimeException e) {
            return SkipDecision.ofSkipped("DateTime not supported: " + e.getMessage());
        }
    }

    static {
        COMMON_SKIP_PATTERN_TO_CHECK.put(
                "neo4j\\.datatypes\\.test_temporal_types\\.TestDataTypes\\.test_should_echo_all_timezone_ids",
                StartSubTest::checkDateTimeSupported);
        COMMON_SKIP_PATTERN_TO_CHECK.put(
                "neo4j\\.datatypes\\.test_temporal_types\\.TestDataTypes\\.test_date_time_cypher_created_tz_id",
                StartSubTest::checkTzIdSupported);

        ASYNC_SKIP_PATTERN_TO_CHECK.putAll(COMMON_SKIP_PATTERN_TO_CHECK);

        REACTIVE_LEGACY_SKIP_PATTERN_TO_CHECK.putAll(COMMON_SKIP_PATTERN_TO_CHECK);

        REACTIVE_SKIP_PATTERN_TO_CHECK.putAll(COMMON_SKIP_PATTERN_TO_CHECK);
    }

    private StartSubTestBody data;

    public static boolean decidePerSubTest(String testName) {
        return skipPatternMatches(testName, COMMON_SKIP_PATTERN_TO_CHECK);
    }

    public static boolean decidePerSubTestAsync(String testName) {
        return skipPatternMatches(testName, ASYNC_SKIP_PATTERN_TO_CHECK);
    }

    public static boolean decidePerSubTestReactiveLegacy(String testName) {
        return skipPatternMatches(testName, REACTIVE_LEGACY_SKIP_PATTERN_TO_CHECK);
    }

    public static boolean decidePerSubTestReactive(String testName) {
        return skipPatternMatches(testName, REACTIVE_SKIP_PATTERN_TO_CHECK);
    }

    private static boolean skipPatternMatches(
            String testName, Map<String, SkipDeciderInterface> skipPatternToFunction) {
        return skipPatternToFunction.entrySet().stream().anyMatch(entry -> testName.matches(entry.getKey()));
    }

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        return createResponse(COMMON_SKIP_PATTERN_TO_CHECK);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        var testkitResponse = createResponse(ASYNC_SKIP_PATTERN_TO_CHECK);
        return CompletableFuture.completedFuture(testkitResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        var testkitResponse = createResponse(REACTIVE_SKIP_PATTERN_TO_CHECK);
        return Mono.just(testkitResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        var testkitResponse = createResponse(REACTIVE_LEGACY_SKIP_PATTERN_TO_CHECK);
        return Mono.just(testkitResponse);
    }

    private TestkitResponse createResponse(Map<String, SkipDeciderInterface> skipPatternToCheck) {
        return skipPatternToCheck.entrySet().stream()
                .filter(entry -> data.getTestName().matches(entry.getKey()))
                .findFirst()
                .map(entry -> {
                    var decision = entry.getValue().check(data.getSubtestArguments());
                    if (decision.skipped()) {
                        return SkipTest.builder()
                                .data(SkipTest.SkipTestBody.builder()
                                        .reason(decision.reason())
                                        .build())
                                .build();
                    }
                    return RunTest.builder().build();
                })
                .orElse(RunTest.builder().build());
    }

    @Setter
    @Getter
    public static class StartSubTestBody {
        private String testName;
        private Map<String, Object> subtestArguments;
    }
}
