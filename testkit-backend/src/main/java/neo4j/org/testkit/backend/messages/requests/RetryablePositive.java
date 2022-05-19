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

import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class RetryablePositive implements TestkitRequest {
    private RetryablePositiveBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        SessionHolder sessionHolder = testkitState.getSessionHolder(data.sessionId);
        if (sessionHolder == null) {
            throw new RuntimeException("Could not find session");
        }
        sessionHolder.getTxWorkFuture().complete(null);
        return null;
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState.getAsyncSessionHolder(data.getSessionId()).thenApply(sessionHolder -> {
            sessionHolder.getTxWorkFuture().complete(null);
            return null;
        });
    }

    @Override
    public Mono<TestkitResponse> processRx(TestkitState testkitState) {
        return testkitState.getRxSessionHolder(data.getSessionId()).mapNotNull(sessionHolder -> {
            sessionHolder.getTxWorkFuture().complete(null);
            return null;
        });
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState.getReactiveSessionHolder(data.getSessionId()).mapNotNull(sessionHolder -> {
            sessionHolder.getTxWorkFuture().complete(null);
            return null;
        });
    }

    @Setter
    @Getter
    public static class RetryablePositiveBody {
        private String sessionId;
    }
}
