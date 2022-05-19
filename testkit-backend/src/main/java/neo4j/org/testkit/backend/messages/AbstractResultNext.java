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
package neo4j.org.testkit.backend.messages;

import java.util.concurrent.CompletionStage;
import neo4j.org.testkit.backend.RxBufferedSubscriber;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.ReactiveResultHolder;
import neo4j.org.testkit.backend.holder.RxResultHolder;
import neo4j.org.testkit.backend.messages.requests.TestkitRequest;
import neo4j.org.testkit.backend.messages.responses.NullRecord;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import reactor.core.publisher.Mono;

public abstract class AbstractResultNext implements TestkitRequest {
    @Override
    public TestkitResponse process(TestkitState testkitState) {
        try {
            Result result = testkitState.getResultHolder(getResultId()).getResult();
            return createResponse(result.next());
        } catch (NoSuchRecordException ignored) {
            return NullRecord.builder().build();
        }
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return testkitState
                .getAsyncResultHolder(getResultId())
                .thenCompose(
                        resultCursorHolder -> resultCursorHolder.getResult().nextAsync())
                .thenApply(this::createResponseNullSafe);
    }

    @Override
    public Mono<TestkitResponse> processRx(TestkitState testkitState) {
        return testkitState.getRxResultHolder(getResultId()).flatMap(resultHolder -> {
            RxBufferedSubscriber<Record> subscriber = resultHolder
                    .getSubscriber()
                    .orElseGet(() -> {
                        RxBufferedSubscriber<Record> subscriberInstance =
                                new RxBufferedSubscriber<>(getFetchSize(resultHolder));
                        resultHolder.setSubscriber(subscriberInstance);
                        resultHolder.getResult().records().subscribe(subscriberInstance);
                        return subscriberInstance;
                    });
            return subscriber
                    .next()
                    .map(this::createResponse)
                    .defaultIfEmpty(NullRecord.builder().build());
        });
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return testkitState.getReactiveResultHolder(getResultId()).flatMap(resultHolder -> {
            RxBufferedSubscriber<Record> subscriber = resultHolder
                    .getSubscriber()
                    .orElseGet(() -> {
                        RxBufferedSubscriber<Record> subscriberInstance =
                                new RxBufferedSubscriber<>(getFetchSize(resultHolder));
                        resultHolder.setSubscriber(subscriberInstance);
                        resultHolder.getResult().records().subscribe(subscriberInstance);
                        return subscriberInstance;
                    });
            return subscriber
                    .next()
                    .map(this::createResponse)
                    .defaultIfEmpty(NullRecord.builder().build());
        });
    }

    protected abstract neo4j.org.testkit.backend.messages.responses.TestkitResponse createResponse(Record record);

    protected abstract String getResultId();

    private neo4j.org.testkit.backend.messages.responses.TestkitResponse createResponseNullSafe(Record record) {
        return record != null ? createResponse(record) : NullRecord.builder().build();
    }

    private long getFetchSize(RxResultHolder resultHolder) {
        long fetchSize = resultHolder
                .getSessionHolder()
                .getConfig()
                .fetchSize()
                .orElse(resultHolder
                        .getSessionHolder()
                        .getDriverHolder()
                        .getConfig()
                        .fetchSize());
        return fetchSize == -1 ? Long.MAX_VALUE : fetchSize;
    }

    private long getFetchSize(ReactiveResultHolder resultHolder) {
        long fetchSize = resultHolder
                .getSessionHolder()
                .getConfig()
                .fetchSize()
                .orElse(resultHolder
                        .getSessionHolder()
                        .getDriverHolder()
                        .getConfig()
                        .fetchSize());
        return fetchSize == -1 ? Long.MAX_VALUE : fetchSize;
    }
}
