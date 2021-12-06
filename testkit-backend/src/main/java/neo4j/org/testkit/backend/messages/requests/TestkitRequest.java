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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;

@JsonTypeInfo( use = JsonTypeInfo.Id.NAME, property = "name" )
@JsonSubTypes( {
        @JsonSubTypes.Type( NewDriver.class ), @JsonSubTypes.Type( NewSession.class ),
        @JsonSubTypes.Type( SessionRun.class ), @JsonSubTypes.Type( ResultNext.class ),
        @JsonSubTypes.Type( ResultConsume.class ), @JsonSubTypes.Type( VerifyConnectivity.class ),
        @JsonSubTypes.Type( SessionClose.class ), @JsonSubTypes.Type( DriverClose.class ),
        @JsonSubTypes.Type( RetryableNegative.class ), @JsonSubTypes.Type( SessionReadTransaction.class ),
        @JsonSubTypes.Type( TransactionRun.class ), @JsonSubTypes.Type( RetryablePositive.class ),
        @JsonSubTypes.Type( SessionBeginTransaction.class ), @JsonSubTypes.Type( TransactionCommit.class ),
        @JsonSubTypes.Type( SessionLastBookmarks.class ), @JsonSubTypes.Type( SessionWriteTransaction.class ),
        @JsonSubTypes.Type( ResolverResolutionCompleted.class ), @JsonSubTypes.Type( CheckMultiDBSupport.class ),
        @JsonSubTypes.Type( DomainNameResolutionCompleted.class ), @JsonSubTypes.Type( StartTest.class ),
        @JsonSubTypes.Type( TransactionRollback.class ), @JsonSubTypes.Type( GetFeatures.class ),
        @JsonSubTypes.Type( GetRoutingTable.class ), @JsonSubTypes.Type( TransactionClose.class ),
        @JsonSubTypes.Type( ResultList.class ), @JsonSubTypes.Type( GetConnectionPoolMetrics.class )
} )
public interface TestkitRequest
{
    TestkitResponse process( TestkitState testkitState );

    CompletionStage<TestkitResponse> processAsync( TestkitState testkitState );

    Mono<TestkitResponse> processRx( TestkitState testkitState );
}
