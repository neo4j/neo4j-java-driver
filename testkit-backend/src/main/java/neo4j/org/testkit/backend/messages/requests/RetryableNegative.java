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
import neo4j.org.testkit.backend.FrontendError;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class RetryableNegative implements TestkitRequest {
    private RetryableNegativeBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        var sessionHolder = testkitState.getSessionHolder(data.sessionId);
        Throwable throwable;
        if (!"".equals(data.getErrorId())) {
            throwable = testkitState.getErrors().get(data.getErrorId());
        } else {
            throwable = new FrontendError();
        }
        sessionHolder.getTxWorkFuture().completeExceptionally(throwable);
        return null;
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState.getAsyncSessionHolder(data.getSessionId()).thenApply(sessionHolder -> {
            Throwable throwable;
            if (!"".equals(data.getErrorId())) {
                throwable = testkitState.getErrors().get(data.getErrorId());
            } else {
                throwable = new FrontendError();
            }
            sessionHolder.getTxWorkFuture().completeExceptionally(throwable);
            return null;
        });
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState.getReactiveSessionHolder(data.getSessionId()).mapNotNull(sessionHolder -> {
            Throwable throwable;
            if (!"".equals(data.getErrorId())) {
                throwable = testkitState.getErrors().get(data.getErrorId());
            } else {
                throwable = new FrontendError();
            }
            sessionHolder.getTxWorkFuture().completeExceptionally(throwable);
            return null;
        });
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return testkitState.getReactiveSessionStreamsHolder(data.getSessionId()).mapNotNull(sessionHolder -> {
            Throwable throwable;
            if (!"".equals(data.getErrorId())) {
                throwable = testkitState.getErrors().get(data.getErrorId());
            } else {
                throwable = new FrontendError();
            }
            sessionHolder.getTxWorkFuture().completeExceptionally(throwable);
            return null;
        });
    }

    @Setter
    @Getter
    public static class RetryableNegativeBody {
        private String sessionId;
        private String errorId;
    }
}
