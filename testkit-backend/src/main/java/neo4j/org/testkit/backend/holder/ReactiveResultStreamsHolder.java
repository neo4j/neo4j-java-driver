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
package neo4j.org.testkit.backend.holder;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.RxBufferedSubscriber;
import org.neo4j.driver.Record;
import org.neo4j.driver.reactivestreams.ReactiveResult;

public class ReactiveResultStreamsHolder
        extends AbstractResultHolder<ReactiveSessionStreamsHolder, ReactiveTransactionStreamsHolder, ReactiveResult> {
    @Setter
    private RxBufferedSubscriber<Record> subscriber;

    @Getter
    private final AtomicLong requestedRecordsCounter = new AtomicLong();

    public ReactiveResultStreamsHolder(ReactiveSessionStreamsHolder sessionHolder, ReactiveResult result) {
        super(sessionHolder, result);
    }

    public ReactiveResultStreamsHolder(ReactiveTransactionStreamsHolder transactionHolder, ReactiveResult result) {
        super(transactionHolder, result);
    }

    public Optional<RxBufferedSubscriber<Record>> getSubscriber() {
        return Optional.ofNullable(subscriber);
    }

    @Override
    protected ReactiveSessionStreamsHolder getSessionHolder(ReactiveTransactionStreamsHolder transactionHolder) {
        return transactionHolder.getSessionHolder();
    }
}
