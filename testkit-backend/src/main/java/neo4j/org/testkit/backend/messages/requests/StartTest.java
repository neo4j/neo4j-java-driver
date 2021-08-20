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
        ASYNC_SKIP_PATTERN_TO_REASON.put( "^.*.test_should_reject_server_using_verify_connectivity_bolt_3x0$", "Does not error as expected" );
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
