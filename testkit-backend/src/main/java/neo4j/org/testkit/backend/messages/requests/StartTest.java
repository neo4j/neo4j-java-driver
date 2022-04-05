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
    private static final Map<String,String> COMMON_SKIP_PATTERN_TO_REASON = new HashMap<>();
    private static final Map<String,String> ASYNC_SKIP_PATTERN_TO_REASON = new HashMap<>();
    private static final Map<String,String> REACTIVE_SKIP_PATTERN_TO_REASON = new HashMap<>();

    static
    {
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_no_notifications$", "An empty list is returned when there are no notifications" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_no_notification_info$", "An empty list is returned when there are no notifications" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_notifications_without_position$", "Null value is provided when position is absent" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_multiple_notifications$", "Null value is provided when position is absent" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_partial_summary_not_contains_system_updates$", "Contains updates because value is over zero" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_partial_summary_not_contains_updates$", "Contains updates because value is over zero" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_profile$", "Missing stats are reported with 0 value" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_server_info$", "Address includes domain name" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_partial_summary_contains_system_updates$", "Does not contain updates because value is zero" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_partial_summary_contains_updates$", "Does not contain updates because value is zero" );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.test_supports_multi_db$", "Database is None" );
        String skipMessage = "This test expects hostname verification to be turned off when all certificates are trusted";
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestTrustAllCertsConfig\\.test_trusted_ca_wrong_hostname$", skipMessage );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestTrustAllCertsConfig\\.test_untrusted_ca_wrong_hostname$", skipMessage );
        skipMessage = "Driver handles connection acquisition timeout differently";
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestConnectionAcquisitionTimeoutMs\\.test_should_encompass_the_handshake_time.*$", skipMessage );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestConnectionAcquisitionTimeoutMs\\.test_should_fail_when_acquisition_timeout_is_reached_first.*$",
                                           skipMessage );
        skipMessage = "This test needs updating to implement expected behaviour";
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestAuthenticationSchemes\\.test_custom_scheme_empty$", skipMessage );
        skipMessage = "Driver does not implement optimization for qid in explicit transaction";
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestOptimizations\\.test_uses_implicit_default_arguments$", skipMessage );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestOptimizations\\.test_uses_implicit_default_arguments_multi_query$", skipMessage );
        COMMON_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestOptimizations\\.test_uses_implicit_default_arguments_multi_query_nested$", skipMessage );

        ASYNC_SKIP_PATTERN_TO_REASON.putAll( COMMON_SKIP_PATTERN_TO_REASON );

        REACTIVE_SKIP_PATTERN_TO_REASON.putAll( COMMON_SKIP_PATTERN_TO_REASON );
        // Current limitations (require further investigation or bug fixing)
        skipMessage = "Does not support multiple concurrent result streams on session level";
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestSessionRun\\.test_iteration_nested$", skipMessage );
        REACTIVE_SKIP_PATTERN_TO_REASON.put( "^.*\\.TestSessionRun\\.test_partial_iteration$", skipMessage );
    }

    private StartTestBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        return createResponse( COMMON_SKIP_PATTERN_TO_REASON );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        TestkitResponse testkitResponse = createResponse( ASYNC_SKIP_PATTERN_TO_REASON );
        return CompletableFuture.completedFuture( testkitResponse );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        TestkitResponse testkitResponse = createResponse( REACTIVE_SKIP_PATTERN_TO_REASON );
        return Mono.fromCompletionStage( CompletableFuture.completedFuture( testkitResponse ) );
    }

    private TestkitResponse createResponse( Map<String,String> skipPatternToReason )
    {
        return skipPatternToReason
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
    }

    @Setter
    @Getter
    public static class StartTestBody
    {
        private String testName;
    }
}
