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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.CustomDriverError;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherParamDeserializer;
import org.neo4j.driver.TransactionConfig;

@Setter
@Getter
abstract class AbstractTestkitRequestWithTransactionConfig<
                T extends AbstractTestkitRequestWithTransactionConfig.TransactionConfigBody>
        implements TestkitRequest {
    protected T data;

    protected TransactionConfig buildTxConfig() {
        return configureTx(TransactionConfig.builder()).build();
    }

    private TransactionConfig.Builder configureTx(TransactionConfig.Builder builder) {
        return configureTxMetadata(configureTxTimeout(builder));
    }

    private TransactionConfig.Builder configureTxMetadata(TransactionConfig.Builder builder) {
        data.getTxMeta().ifPresent(builder::withMetadata);
        return builder;
    }

    private TransactionConfig.Builder configureTxTimeout(TransactionConfig.Builder builder) {
        try {
            data.getTimeout().ifPresent((timeout) -> builder.withTimeout(Duration.ofMillis(timeout)));
        } catch (IllegalArgumentException e) {
            throw new CustomDriverError(e);
        }
        return builder;
    }

    @Setter
    abstract static class TransactionConfigBody {
        protected Integer timeout;

        @JsonDeserialize(using = TestkitCypherParamDeserializer.class)
        protected Map<String, Object> txMeta;

        Optional<Integer> getTimeout() {
            return Optional.ofNullable(timeout);
        }

        Optional<Map<String, Object>> getTxMeta() {
            return Optional.ofNullable(txMeta);
        }
    }
}
