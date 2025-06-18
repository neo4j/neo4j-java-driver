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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.RunSubTests;
import neo4j.org.testkit.backend.messages.responses.RunTest;
import neo4j.org.testkit.backend.messages.responses.SkipTest;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class StartTest implements TestkitRequest {
    private static final Map<String, String> COMMON_SKIP_PATTERN_TO_REASON = new HashMap<>();
    private static final Map<String, String> SYNC_SKIP_PATTERN_TO_REASON = new HashMap<>();
    private static final Map<String, String> ASYNC_SKIP_PATTERN_TO_REASON = new HashMap<>();
    private static final Map<String, String> REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON = new HashMap<>();
    private static final Map<String, String> REACTIVE_SKIP_PATTERN_TO_REASON = new HashMap<>();

    static {
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.test_no_notifications$", "An empty list is returned when there are no notifications");
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.test_no_notification_info$", "An empty list is returned when there are no notifications");
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.test_notifications_without_position$", "Null value is provided when position is absent");
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.test_multiple_notifications$", "Null value is provided when position is absent");
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.test_partial_summary_not_contains_system_updates$",
                "Contains updates because value is over zero");
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.test_partial_summary_not_contains_updates$", "Contains updates because value is over zero");
        COMMON_SKIP_PATTERN_TO_REASON.put("^.*\\.test_profile$", "Missing stats are reported with 0 value");
        COMMON_SKIP_PATTERN_TO_REASON.put("^.*\\.test_server_info$", "Address includes domain name");
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.test_partial_summary_contains_system_updates$",
                "Does not contain updates because value is zero");
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.test_partial_summary_contains_updates$", "Does not contain updates because value is zero");
        COMMON_SKIP_PATTERN_TO_REASON.put("^.*\\.test_supports_multi_db$", "Database is None");
        var skipMessage = "Driver handles connection acquisition timeout differently";
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestConnectionAcquisitionTimeoutMs\\.test_should_encompass_the_handshake_time.*$", skipMessage);
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestConnectionAcquisitionTimeoutMs\\.test_router_handshake_has_own_timeout_too_slow$",
                skipMessage);
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestConnectionAcquisitionTimeoutMs\\.test_should_fail_when_acquisition_timeout_is_reached_first.*$",
                skipMessage);
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestConnectionAcquisitionTimeoutMs\\.test_should_encompass_the_version_handshake_(in_time|time_out)$",
                skipMessage);
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestHomeDbMixedCluster\\.test_connection_acquisition_timeout_during_fallback$", skipMessage);
        skipMessage = "This test needs updating to implement expected behaviour";
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthenticationSchemes[^.]+\\.test_custom_scheme_empty$", skipMessage);
        skipMessage = "Driver does not implement optimization for qid in explicit transaction";
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestOptimizations\\.test_uses_implicit_default_arguments$", skipMessage);
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestOptimizations\\.test_uses_implicit_default_arguments_multi_query$", skipMessage);
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestOptimizations\\.test_uses_implicit_default_arguments_multi_query_nested$", skipMessage);
        COMMON_SKIP_PATTERN_TO_REASON.put("^.*\\.TestResultSingle\\.test_result_single_with_2_records$", skipMessage);
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^stub\\.routing\\.test_routing_v[^.]*\\.RoutingV[^.]*\\.test_ipv6_read",
                "Needs trying all DNS resolved addresses for hosts in the routing table");
        COMMON_SKIP_PATTERN_TO_REASON.put(
                "^stub\\.summary\\.test_summary\\.TestSummaryBasicInfoDiscard\\.test_times$",
                "Driver sets summary's resultAvailableAfter to -1 on discard");

        SYNC_SKIP_PATTERN_TO_REASON.putAll(COMMON_SKIP_PATTERN_TO_REASON);
        skipMessage =
                "Background handling of pipelined PULL failure might result in manager notification response being sent before respective Testkit request";
        SYNC_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthTokenManager[^.]+\\.test_notify_on_token_expired_pull_using_session_run$", skipMessage);
        SYNC_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthTokenManager[^.]+\\.test_notify_on_token_expired_pull_using_tx_run$", skipMessage);

        ASYNC_SKIP_PATTERN_TO_REASON.putAll(COMMON_SKIP_PATTERN_TO_REASON);
        ASYNC_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthTokenManager[^.]+\\.test_notify_on_token_expired_pull_using_session_run$", skipMessage);
        ASYNC_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthTokenManager[^.]+\\.test_notify_on_token_expired_pull_using_tx_run$", skipMessage);

        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.putAll(COMMON_SKIP_PATTERN_TO_REASON);
        // Current limitations (require further investigation or bug fixing)
        skipMessage = "Does not report RUN FAILURE";
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.Routing[^.]+\\.test_should_write_successfully_on_leader_switch_using_tx_function$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put("^.*\\.TestDisconnects\\.test_disconnect_after_hello$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestDisconnects\\.test_disconnect_session_on_run$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put("^.*\\.TestDisconnects\\.test_disconnect_on_tx_run$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestSessionRun\\.test_raises_error_on_session_run$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestTxRun\\.test_raises_error_on_tx(_func)?_run", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestTxRun\\.test_failed_tx_run_allows(_skipping)?_rollback", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthorizationV\\dx\\d\\.test_should_fail_with_auth_expired_on_run_using_tx_run$",
                skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthorizationV\\dx\\d\\.test_should_fail_with_token_expired_on_run_using_tx_run$",
                skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestDirectConnectionRecvTimeout\\.test_timeout$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestDirectConnectionRecvTimeout\\.test_timeout_unmanaged_tx$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestDirectConnectionRecvTimeout\\.test_timeout_unmanaged_tx_should_fail_subsequent_usage_after_timeout$",
                skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestDirectConnectionRecvTimeout\\.test_timeout_managed_tx_retry$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestRoutingConnectionRecvTimeout\\.test_timeout$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestRoutingConnectionRecvTimeout\\.test_timeout_unmanaged_tx$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestRoutingConnectionRecvTimeout\\.test_timeout_unmanaged_tx_should_fail_subsequent_usage_after_timeout$",
                skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestRoutingConnectionRecvTimeout\\.test_timeout_managed_tx_retry$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestTxRun\\.test_broken_transaction_should_not_break_session$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestTxRun\\.test_does_not_update_last_bookmark_on_failure$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthTokenManager[^.]+\\.test_not_notify_on_auth_expired_run_using_tx_run$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthTokenManager[^.]+\\.test_notify_on_token_expired_run_using_tx_run$", skipMessage);
        skipMessage = "The expects run failure to be reported immediately on run method";
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.Routing[^.]+\\.test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_run_using_tx_run$",
                skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestTxRun\\.test_should_prevent_pull_after_tx_termination_on_run$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestTxRun\\.test_should_prevent_discard_after_tx_termination_on_run$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestTxRun\\.test_should_prevent_run_after_tx_termination_on_run$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestTxRun\\.test_should_prevent_run_after_tx_termination_on_pull$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put(
                "^.*\\.TestAuthTokenManager[^.]+\\.test_error_on_run_using_tx_run$", skipMessage);
        skipMessage = "Does not support multiple concurrent result streams on session level";
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put("^.*\\.TestSessionRun\\.test_iteration_nested$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put("^.*\\.TestSessionRun\\.test_partial_iteration$", skipMessage);
        REACTIVE_LEGACY_SKIP_PATTERN_TO_REASON.put("^.*\\.TestIterationSessionRun\\.test_nested$", skipMessage);

        REACTIVE_SKIP_PATTERN_TO_REASON.putAll(COMMON_SKIP_PATTERN_TO_REASON);

        REACTIVE_SKIP_PATTERN_TO_REASON.put("^.*\\.TestSessionRun\\.test_iteration_nested$", skipMessage);
        REACTIVE_SKIP_PATTERN_TO_REASON.put("^.*\\.TestSessionRun\\.test_partial_iteration$", skipMessage);
        REACTIVE_SKIP_PATTERN_TO_REASON.put("^.*\\.TestIterationSessionRun\\.test_nested$", skipMessage);
    }

    private StartTestBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        return createSkipResponse(SYNC_SKIP_PATTERN_TO_REASON)
                .orElseGet(() -> StartSubTest.decidePerSubTestReactive(data.getTestName())
                        ? RunSubTests.builder().build()
                        : RunTest.builder().build());
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        var testkitResponse = createSkipResponse(ASYNC_SKIP_PATTERN_TO_REASON)
                .orElseGet(() -> StartSubTest.decidePerSubTestReactive(data.getTestName())
                        ? RunSubTests.builder().build()
                        : RunTest.builder().build());
        return CompletableFuture.completedFuture(testkitResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        var testkitResponse = createSkipResponse(REACTIVE_SKIP_PATTERN_TO_REASON)
                .orElseGet(() -> StartSubTest.decidePerSubTestReactive(data.getTestName())
                        ? RunSubTests.builder().build()
                        : RunTest.builder().build());
        return Mono.just(testkitResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        var testkitResponse = createSkipResponse(REACTIVE_SKIP_PATTERN_TO_REASON)
                .orElseGet(() -> StartSubTest.decidePerSubTestReactive(data.getTestName())
                        ? RunSubTests.builder().build()
                        : RunTest.builder().build());
        return Mono.just(testkitResponse);
    }

    private Optional<TestkitResponse> createSkipResponse(Map<String, String> skipPatternToReason) {
        return skipPatternToReason.entrySet().stream()
                .filter(entry -> data.getTestName().matches(entry.getKey()))
                .findFirst()
                .map(entry -> SkipTest.builder()
                        .data(SkipTest.SkipTestBody.builder()
                                .reason(entry.getValue())
                                .build())
                        .build());
    }

    @Setter
    @Getter
    public static class StartTestBody {
        private String testName;
    }
}
