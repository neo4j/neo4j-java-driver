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
import java.util.Map;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.deserializer.HexByteArrayDeserializer;
import neo4j.org.testkit.backend.messages.responses.EncapsulatedKey;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.internal.property_encryption.AbstractEncapsulatedKeyManager;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class ImportEncapsulatedKey implements TestkitRequest {
    private ImportEncapsulatedKeyBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption();
        var keyManager = encryption.keyManager(data.getProfileName());
        try {
            var field = AbstractEncapsulatedKeyManager.class.getDeclaredField("keyRepository");
            field.setAccessible(true);
            var repository = (NewDriver.InMemoryKeyRepository) field.get(keyManager);
            var key = repository
                    .save(data.getKeyId(), data.getAlias(), data.getEncapsulation(), data.getMetadata())
                    .toCompletableFuture()
                    .join();
            return EncapsulatedKey.builder()
                    .data(EncapsulatedKey.EncapsulatedKeyBody.builder()
                            .id(key.id())
                            .alias(key.alias().orElse(null))
                            .build())
                    .build();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return null;
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return null;
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return null;
    }

    @Setter
    @Getter
    public static class ImportEncapsulatedKeyBody {
        private String driverId;
        private String keyId;
        private String alias;

        @JsonDeserialize(using = HexByteArrayDeserializer.class)
        private byte[] encapsulation;

        private String profileName;
        private Map<String, String> metadata;
    }
}
