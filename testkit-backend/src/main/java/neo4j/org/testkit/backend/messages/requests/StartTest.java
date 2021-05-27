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

    static
    {
        SKIP_PATTERN_TO_REASON.put( "^.*retry.TestRetryClustering.test_retry_database_unavailable$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.*retry.TestRetryClustering.test_retry_made_up_transient$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.*retry.TestRetryClustering.test_retry_ForbiddenOnReadOnlyDatabase$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.*retry.TestRetryClustering.test_retry_NotALeader$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON
                .put( "^.*retry.TestRetryClustering.test_retry_ForbiddenOnReadOnlyDatabase_ChangingWriter$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.*routing.Routing\\..+$", "The tests are not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON
                .put( "^.+routing.Routing.*\\.test_should_successfully_get_server_protocol_version$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.+routing.Routing.*\\.test_should_successfully_get_server_agent$", "The test is not applicable to 4.2 driver" );
        SKIP_PATTERN_TO_REASON.put( "^.*authorization.AuthorizationTests.*", "These tests is not applicable to 4.2 driver" );
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
