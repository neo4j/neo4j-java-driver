/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;

import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.NullRecord;
import neo4j.org.testkit.backend.messages.responses.Summary;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.summary.ResultSummary;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class ResultConsume implements TestkitRequest {
    private ResultConsumeBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        try {
            var result = testkitState.getResultHolder(data.getResultId()).getResult();
            return createResponse(result.consume());
        } catch (NoSuchRecordException ignored) {
            return NullRecord.builder().build();
        }
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncResultHolder(data.getResultId())
                .thenCompose(
                        resultCursorHolder -> resultCursorHolder.getResult().consumeAsync())
                .thenApply(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState
                .getReactiveResultHolder(data.getResultId())
                .flatMap(resultHolder -> Mono.fromDirect(
                        flowPublisherToFlux(resultHolder.getResult().consume())))
                .map(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState
                .getReactiveResultStreamsHolder(data.getResultId())
                .flatMap(
                        resultHolder -> Mono.fromDirect(resultHolder.getResult().consume()))
                .map(this::createResponse);
    }

    private Summary createResponse(ResultSummary summary) {
        return Summary.builder().data(SummaryUtil.toSummaryBody(summary)).build();
    }

    @Setter
    @Getter
    public static class ResultConsumeBody {
        private String resultId;
    }
}
