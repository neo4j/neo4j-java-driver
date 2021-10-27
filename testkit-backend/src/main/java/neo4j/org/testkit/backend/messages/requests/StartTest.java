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

import java.util.LinkedHashMap;
import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
public class StartTest implements TestkitRequest
{
    private static final Map<String,String> SKIP_PATTERN_TO_REASON = new LinkedHashMap<>();

    private static final String SERVER_INFO_SKIP_REASON_MESSAGE =
            "The 4.2 driver backend does not provide server info and its properties were changed in 4.3 drivers";

    static
    {
        SKIP_PATTERN_TO_REASON.put( "^.*retry\\.TestRetryClustering\\.test_retry_database_unavailable$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.*retry\\.TestRetryClustering\\.test_retry_made_up_transient$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.*retry\\.TestRetryClustering\\.test_retry_ForbiddenOnReadOnlyDatabase$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.*retry\\.TestRetryClustering\\.test_retry_NotALeader$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON
                .put( "^.*retry\\.TestRetryClustering\\.test_retry_ForbiddenOnReadOnlyDatabase_ChangingWriter$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.*test_routing_v4x3\\.RoutingV4x3\\..+$", "The tests are not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON
                .put( "^.+routing\\.Routing.*\\.test_should_successfully_get_server_protocol_version$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.+routing\\.Routing.*\\.test_should_successfully_get_server_agent$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.+disconnects\\.TestDisconnects\\.test_client_says_goodbye$", "This test uses 4.3 Bolt" );
        SKIP_PATTERN_TO_REASON.put( "^.+disconnects\\.TestDisconnects\\.test_disconnect_on_hello", "This test uses 4.3 Bolt" );
        SKIP_PATTERN_TO_REASON.put( "^.+disconnects\\.TestDisconnects\\.test_disconnect_after_hello", "This test uses 4.3 Bolt" );
        SKIP_PATTERN_TO_REASON.put( "^.+disconnects\\.TestDisconnects\\.test_disconnect_on_tx_begin", "The 4.2 driver disconnects after first next" );
        SKIP_PATTERN_TO_REASON.put( "^.+disconnects\\.TestDisconnects\\.test_disconnect_on_tx_run", "The 4.2 driver disconnects after first next" );
        SKIP_PATTERN_TO_REASON.put( "^.+disconnects\\.TestDisconnects\\.test_disconnect_session_on_run", "The 4.2 driver disconnects after first next" );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing\\.NoRouting\\.test_should_read_successfully_using_session_run", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON
                .put( "^.+test_routing_v3\\.RoutingV3\\.test_should_read_successfully_from_reader_using_session_run", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON.put( "^.+test_routing_v3\\.RoutingV3\\.test_should_read_successfully_from_reader_using_session_run_with_default_db_driver",
                                    SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON
                .put( "^.+test_routing_v3\\.RoutingV3\\.test_should_read_successfully_from_reader_using_tx_function", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON.put( "^.+test_routing_v3\\.RoutingV3\\.test_should_read_successfully_from_reader_using_tx_run",
                                    SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON
                .put( "^.+test_routing_v3\\.RoutingV3\\.test_should_write_successfully_on_writer_using_session_run", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON
                .put( "^.+test_routing_v3\\.RoutingV3\\.test_should_write_successfully_on_writer_using_tx_function", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON.put( "^.+test_routing_v3\\.RoutingV3\\.test_should_write_successfully_on_writer_using_tx_run", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON
                .put( "^.+test_routing_v4x1\\.RoutingV4x1\\.test_should_read_successfully_from_reader_using_session_run", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON
                .put( "^.+test_routing_v4x1\\.RoutingV4x1\\.test_should_read_successfully_from_reader_using_tx_function", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON
                .put( "^.+test_routing_v4x1\\.RoutingV4x1\\.test_should_read_successfully_from_reader_using_tx_run", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON
                .put( "^.+test_routing_v4x1\\.RoutingV4x1\\.test_should_write_successfully_on_writer_using_session_run", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON
                .put( "^.+test_routing_v4x1\\.RoutingV4x1\\.test_should_write_successfully_on_writer_using_tx_function", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON.put( "^.+test_routing_v4x1\\.RoutingV4x1\\.test_should_write_successfully_on_writer_using_tx_run",
                                    SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON.put( "^.+versions\\.TestProtocolVersions\\.test_server_agent", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON.put( "^.+versions\\.TestProtocolVersions\\.test_server_version", SERVER_INFO_SKIP_REASON_MESSAGE );
        SKIP_PATTERN_TO_REASON.put( "^.+test_retry\\.TestRetry\\.test_no_retry_on_syntax_error$", "This test uses 4.3 Bolt" );
        String skipReason = "Skipped because it needs investigation";
        SKIP_PATTERN_TO_REASON.put( "^.+test_should_fail_when_driver_closed_using_session_run$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_accept_noop_during_records_streaming$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_check_multi_db_support$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_error_on_commit_failure_using_tx_commit$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_error_on_database_shutdown_using_tx_commit$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_error_on_database_shutdown_using_tx_run$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_error_on_rollback_failure_using_session_close$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_error_on_rollback_failure_using_tx_rollback$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_exclude_routing_context$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_read_successfully_using_read_session_run$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_read_successfully_using_write_session_run$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v3\\.NoRoutingV3\\.test_should_send_custom_user_agent_using_write_session_run$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+test_no_routing_v4x1\\.NoRoutingV4x1\\..*$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+TestProtocolVersions\\.test_should_reject_server_using_verify_connectivity_bolt_3x0$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+TestProtocolVersions\\.test_supports_bolt_4x3$", skipReason );
        SKIP_PATTERN_TO_REASON.put( "^.+TestDirectDriver\\.test_multi_db_non_existing$", skipReason );
    }

    private StartTestBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        return SKIP_PATTERN_TO_REASON
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
    @NoArgsConstructor
    public static class StartTestBody
    {
        private String testName;
    }
}
