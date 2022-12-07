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

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import neo4j.org.testkit.backend.CustomDriverError;
import org.neo4j.driver.TransactionConfig;

public abstract class WithTxConfig implements TestkitRequest {
    public abstract ITxConfigBody getData();

    protected TransactionConfig.Builder configureTxTimeout(TransactionConfig.Builder builder) {
        if (!getData().getTimeoutPresent()) return builder;
        try {
            Optional.ofNullable(getData().getTimeout())
                    .ifPresentOrElse(
                            (timeout) -> builder.withTimeout(Duration.ofMillis(timeout)), builder::withDefaultTimeout);
        } catch (IllegalArgumentException e) {
            throw new CustomDriverError(e);
        }
        return builder;
    }

    protected TransactionConfig.Builder configureTxMetadata(TransactionConfig.Builder builder) {
        Optional.ofNullable(getData().getTxMeta()).ifPresent(builder::withMetadata);
        return builder;
    }

    protected TransactionConfig.Builder configureTx(TransactionConfig.Builder builder) {
        return configureTxMetadata(configureTxTimeout(builder));
    }

    protected TransactionConfig getTxConfig() {
        return configureTx(TransactionConfig.builder()).build();
    }

    public interface ITxConfigBody {
        Boolean getTimeoutPresent();

        Integer getTimeout();

        Map<String, Object> getTxMeta();
    }
}
