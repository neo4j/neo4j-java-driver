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
import neo4j.org.testkit.backend.messages.responses.NullRecord;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class ResultPeek implements TestkitRequest {
    private ResultPeekBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        try {
            var result = testkitState.getResultHolder(data.getResultId()).getResult();
            return createResponse(result.peek());
        } catch (NoSuchRecordException ignored) {
            return NullRecord.builder().build();
        }
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncResultHolder(data.getResultId())
                .thenCompose(
                        resultCursorHolder -> resultCursorHolder.getResult().peekAsync())
                .thenApply(this::createResponseNullSafe);
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    private TestkitResponse createResponse(Record record) {
        return neo4j.org.testkit.backend.messages.responses.Record.builder()
                .data(neo4j.org.testkit.backend.messages.responses.Record.RecordBody.builder()
                        .values(record)
                        .build())
                .build();
    }

    private TestkitResponse createResponseNullSafe(Record record) {
        return record != null ? createResponse(record) : NullRecord.builder().build();
    }

    @Setter
    @Getter
    public static class ResultPeekBody {
        private String resultId;
    }
}
