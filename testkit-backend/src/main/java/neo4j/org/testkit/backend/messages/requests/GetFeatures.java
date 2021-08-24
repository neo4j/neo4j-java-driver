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
import neo4j.org.testkit.backend.messages.responses.FeatureList;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Setter
@Getter
public class GetFeatures implements TestkitRequest
{
    private static final Set<String> COMMON_FEATURES = new HashSet<>( Arrays.asList(
            "AuthorizationExpiredTreatment",
            "Optimization:PullPipelining",
            "ConfHint:connection.recv_timeout_seconds",
            "Temporary:DriverFetchSize",
            "Temporary:DriverMaxTxRetryTime",
            "Temporary:ResultList"
    ) );

    private static final Set<String> SYNC_FEATURES = new HashSet<>( Collections.singletonList(
            "Temporary:TransactionClose"
    ) );

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        Set<String> features = new HashSet<>( COMMON_FEATURES );
        features.addAll( SYNC_FEATURES );
        return createResponse( features );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return CompletableFuture.completedFuture( createResponse( COMMON_FEATURES ) );
    }

    private FeatureList createResponse( Set<String> features )
    {
        return FeatureList.builder().data( FeatureList.FeatureListBody.builder().features( features ).build() ).build();
    }
}
