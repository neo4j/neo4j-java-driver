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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherParamDeserializer;
import neo4j.org.testkit.backend.messages.responses.Result;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;

@Setter
@Getter
@NoArgsConstructor
public class TransactionRun implements TestkitRequest
{
    private TransactionRunBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        org.neo4j.driver.Result result =
                testkitState.getTransaction( data.getTxId() ).run( data.getCypher(), data.getParams() != null ? data.getParams() : Collections.emptyMap() );
        String resultId = testkitState.newId();
        testkitState.getResults().put( resultId, result );
        return createResponse( resultId );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return testkitState.getAsyncTransaction( data.getTxId() )
                .thenCompose( tx -> tx.runAsync( data.getCypher(), data.getParams() != null ? data.getParams() : Collections.emptyMap() ))
                .thenApply( resultCursor ->
                {
                    String resultId = testkitState.newId();
                    testkitState.getResultCursors().put( resultId, resultCursor );
                    return createResponse( resultId );
                } ) ;
    }

    private Result createResponse( String resultId )
    {
        return Result.builder().data( Result.ResultBody.builder().id( resultId ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class TransactionRunBody
    {
        private String txId;
        private String cypher;
        @JsonDeserialize( using = TestkitCypherParamDeserializer.class )
        private Map<String,Object> params;
    }
}
