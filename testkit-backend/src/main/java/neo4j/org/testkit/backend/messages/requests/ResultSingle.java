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

import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.ResultCursorHolder;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Record;
import org.neo4j.driver.async.ResultCursor;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class ResultSingle implements TestkitRequest {
    private ResultSingleBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        return createResponse(
                testkitState.getResultHolder(data.getResultId()).getResult().single());
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncResultHolder(data.getResultId())
                .thenApply(ResultCursorHolder::getResult)
                .thenCompose(ResultCursor::singleAsync)
                .thenApply(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        throw new UnsupportedOperationException("Single method is not supported by reactive API");
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        throw new UnsupportedOperationException("Single method is not supported by reactive API");
    }

    private neo4j.org.testkit.backend.messages.responses.TestkitResponse createResponse(Record record) {
        return neo4j.org.testkit.backend.messages.responses.Record.builder()
                .data(neo4j.org.testkit.backend.messages.responses.Record.RecordBody.builder()
                        .values(record)
                        .build())
                .build();
    }

    @Setter
    @Getter
    public static class ResultSingleBody {
        private String resultId;
    }
}
