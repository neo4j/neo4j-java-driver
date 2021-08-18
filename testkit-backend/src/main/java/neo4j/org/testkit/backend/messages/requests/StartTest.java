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
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.RunTest;
import neo4j.org.testkit.backend.messages.responses.SkipTest;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Setter
@Getter
@NoArgsConstructor
public class StartTest implements TestkitRequest
{
    private static final Map<String,String> ASYNC_SKIP_PATTERN_TO_REASON = new HashMap<>();

    static
    {
        ASYNC_SKIP_PATTERN_TO_REASON.put( "^.*.test_should_fail_when_driver_closed_using_session_run$", "Does not throw error" );
        ASYNC_SKIP_PATTERN_TO_REASON.put( "^.*.test_should_read_successfully_on_empty_discovery_result_using_session_run$", "Resolver not implemented" );
        ASYNC_SKIP_PATTERN_TO_REASON.put( "^.*.test_should_request_rt_from_all_initial_routers_until_successful", "Resolver not implemented" );
        ASYNC_SKIP_PATTERN_TO_REASON.put( "^.*.test_should_revert_to_initial_router_if_known_router_throws_protocol_errors", "Resolver not implemented" );
        ASYNC_SKIP_PATTERN_TO_REASON.put( "^.*.test_should_successfully_acquire_rt_when_router_ip_changes$", "Resolver not implemented" );
        ASYNC_SKIP_PATTERN_TO_REASON.put( "^.*.test_should_use_resolver_during_rediscovery_when_existing_routers_fail$", "Resolver not implemented" );
        ASYNC_SKIP_PATTERN_TO_REASON.put( "^.*.test_should_reject_server_using_verify_connectivity_bolt_3x0", "Does not error as expected" );
    }

    private StartTestBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        return RunTest.builder().build();
    }

    @Override
    public CompletionStage<Optional<TestkitResponse>> processAsync( TestkitState testkitState )
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

        return CompletableFuture.completedFuture( Optional.of( testkitResponse ) );
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class StartTestBody
    {
        private String testName;
    }
}
