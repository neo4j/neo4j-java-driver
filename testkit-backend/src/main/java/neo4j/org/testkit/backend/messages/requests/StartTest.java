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

import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.RunTest;
import neo4j.org.testkit.backend.messages.responses.SkipTest;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Setter
@Getter
public class StartTest implements TestkitRequest
{
    private static final Map<String,String> ASYNC_SKIP_PATTERN_TO_REASON = new HashMap<>();
    private static final Map<String,String> REACTIVE_SKIP_PATTERN_TO_REASON = new HashMap<>();

    static
    {
        ASYNC_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_should_reject_server_using_verify_connectivity_bolt_3x0$", "Does not error as expected" );

        // V3 tests
        String skipMessage = "v3 is not applicable to reactive";
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestAuthorizationV3\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestBookmarksV3\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.NoRoutingV3\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.RoutingV3\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestProtocolVersions\\.test_should_reject_server_using_verify_connectivity_bolt_3x0$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestProtocolVersions\\.test_supports_bolt_3x0", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestIterationSessionRun\\.test_all_v3$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestIterationSessionRun\\.test_discards_on_session_close$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestIterationTxRun\\.test_batch_v3$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestIterationTxRun\\.test_all_v3$", skipMessage );

        // Current limitations (require further investigation or bug fixing)
        skipMessage = "Does not report RUN FAILURE";
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.Routing[^.]+\\.test_should_write_successfully_on_leader_switch_using_tx_function$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestDisconnects\\.test_disconnect_after_hello$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestDisconnects\\.test_disconnect_session_on_run$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestDisconnects\\.test_disconnect_on_tx_run$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.NoRouting[^.]+\\.test_should_error_on_database_shutdown_using_tx_run$", "Session close throws error" );
        skipMessage = "Requires investigation";
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestProtocolVersions\\.test_server_agent", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestProtocolVersions\\.test_server_version", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestProtocolVersions\\.test_supports_bolt_[^.]+$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestAuthorizationV4x1\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestAuthorizationV4x3\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestNoRoutingAuthorization\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestOptimizations\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestDirectConnectionRecvTimeout\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestRoutingConnectionRecvTimeout\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestRoutingConnectionRecvTimeout\\.test_timeout$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestRoutingConnectionRecvTimeout\\.test_timeout_unmanaged_tx$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestDisconnects\\.test_disconnect_session_on_tx_commit$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestDisconnects\\.test_fail_on_reset$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestSessionRunParameters\\..*$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestSessionRun\\.test_autocommit_transactions_should_support_timeout$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestSessionRun\\.test_fails_on_bad_syntax$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestSessionRun\\.test_fails_on_missing_parameter$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestSessionRun\\.test_iteration_nested$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestTxFuncRun\\.test_iteration_nested$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestTxRun\\.test_should_not_run_valid_query_in_invalid_tx$", skipMessage );
    }

    private StartTestBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        return RunTest.builder().build();
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        TestkitResponse testkitResponse = ASYNC_SKIP_PATTERN_TO_REASON
                .entrySet()
                .stream()
                .filter( entry -> data.getTestName().matches( entry.getKey() ) )
                .findFirst()
                .map( entry -> (TestkitResponse) SkipTest.builder()
                                                         .data( SkipTest.SkipTestBody.builder()
                                                                                     .reason( entry.getValue() )
                                                                                     .build() )
                                                         .build() )
                .orElseGet( () -> RunTest.builder().build() );

        return CompletableFuture.completedFuture( testkitResponse );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        TestkitResponse testkitResponse = REACTIVE_SKIP_PATTERN_TO_REASON
                .entrySet()
                .stream()
                .filter( entry -> data.getTestName().matches( entry.getKey() ) )
                .findFirst()
                .map( entry -> (TestkitResponse) SkipTest.builder()
                                                         .data( SkipTest.SkipTestBody.builder()
                                                                                     .reason( entry.getValue() )
                                                                                     .build() )
                                                         .build() )
                .orElseGet( () -> RunTest.builder().build() );

        return Mono.fromCompletionStage( CompletableFuture.completedFuture( testkitResponse ) );
    }

    @Setter
    @Getter
    public static class StartTestBody
    {
        private String testName;
    }
}
