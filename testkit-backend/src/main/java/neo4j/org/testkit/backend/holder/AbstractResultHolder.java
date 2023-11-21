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

import lombok.Getter;

public abstract class AbstractResultHolder<T1, T2 extends AbstractTransactionHolder<?, ?>, T3> {
    private final T1 sessionHolder;
    private final T2 transactionHolder;

    @Getter
    private final T3 result;

    public AbstractResultHolder(T1 sessionHolder, T3 result) {
        this.sessionHolder = sessionHolder;
        this.transactionHolder = null;
        this.result = result;
    }

    public AbstractResultHolder(T2 transactionHolder, T3 result) {
        this.sessionHolder = null;
        this.transactionHolder = transactionHolder;
        this.result = result;
    }

    public T1 getSessionHolder() {
        return transactionHolder != null ? getSessionHolder(transactionHolder) : sessionHolder;
    }

    protected abstract T1 getSessionHolder(T2 transactionHolder);
}
