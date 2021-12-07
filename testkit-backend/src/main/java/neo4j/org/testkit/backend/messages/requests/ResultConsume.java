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
import neo4j.org.testkit.backend.messages.responses.NullRecord;
import neo4j.org.testkit.backend.messages.responses.Summary;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.Result;
import org.neo4j.driver.exceptions.NoSuchRecordException;

@Setter
@Getter
public class ResultConsume implements TestkitRequest
{
    private ResultConsumeBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        try
        {
            Result result = testkitState.getResultHolder( data.getResultId() ).getResult();
            return createResponse( result.consume() );
        }
        catch ( NoSuchRecordException ignored )
        {
            return NullRecord.builder().build();
        }
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return testkitState.getAsyncResultHolder( data.getResultId() )
                           .thenCompose( resultCursorHolder -> resultCursorHolder.getResult().consumeAsync() )
                           .thenApply( this::createResponse );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        return testkitState.getRxResultHolder( data.getResultId() )
                           .flatMap( resultHolder -> Mono.fromDirect( resultHolder.getResult().consume() ) )
                           .map( this::createResponse );
    }

    private Summary createResponse( org.neo4j.driver.summary.ResultSummary summary )
    {
        Summary.ServerInfo serverInfo = Summary.ServerInfo.builder()
                                                          .address( summary.server().address() )
                                                          .protocolVersion( summary.server().protocolVersion() )
                                                          .agent( summary.server().agent() )
                                                          .build();
        Summary.SummaryBody data = Summary.SummaryBody.builder()
                                                      .serverInfo( serverInfo )
                                                      .build();
        return Summary.builder()
                      .data( data )
                      .build();
    }

    @Setter
    @Getter
    public static class ResultConsumeBody
    {
        private String resultId;
    }
}
