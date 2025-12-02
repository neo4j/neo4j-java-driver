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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.deserializer.HexByteArrayDeserializer;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.internal.property_encryption.AbstractEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption;
import org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption;
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
        var repository = repository(keyManager);
        var key = repository
                .save(data.getId(), data.getAlias(), data.getEncapsulation(), data.getMetadata())
                .toCompletableFuture()
                .join();
        return createResponse(key);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption(AsyncPropertyEncryption.class);
        var keyManager = encryption.keyManager(data.getProfileName());
        var repository = repository(keyManager);
        var key = repository
                .save(data.getId(), data.getAlias(), data.getEncapsulation(), data.getMetadata())
                .toCompletableFuture()
                .join();
        return CompletableFuture.completedStage(createResponse(key));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption(ReactivePropertyEncryption.class);
        var keyManager = encryption.keyManager(data.getProfileName());
        var repository = repository(keyManager);
        var key = repository
                .save(data.getId(), data.getAlias(), data.getEncapsulation(), data.getMetadata())
                .toCompletableFuture()
                .join();
        return Mono.just(createResponse(key));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption(
                org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption.class);
        var keyManager = encryption.keyManager(data.getProfileName());
        var repository = repository(keyManager);
        var key = repository
                .save(data.getId(), data.getAlias(), data.getEncapsulation(), data.getMetadata())
                .toCompletableFuture()
                .join();
        return Mono.just(createResponse(key));
    }

    private NewDriver.InMemoryKeyRecordRepository repository(Object keyManager) {
        try {
            var field = AbstractEncapsulatedKeyManager.class.getDeclaredField("keyRepository");
            field.setAccessible(true);
            return (NewDriver.InMemoryKeyRecordRepository) field.get(keyManager);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private neo4j.org.testkit.backend.messages.responses.EncapsulatedKey createResponse(EncapsulatedKeyRecord key) {
        return neo4j.org.testkit.backend.messages.responses.EncapsulatedKey.builder()
                .data(neo4j.org.testkit.backend.messages.responses.EncapsulatedKey.EncapsulatedKeyBody.builder()
                        .id(key.id())
                        .alias(key.alias().orElse(null))
                        .build())
                .build();
    }

    @Setter
    @Getter
    public static class ImportEncapsulatedKeyBody {
        private String driverId;
        private String id;
        private String alias;

        @JsonDeserialize(using = HexByteArrayDeserializer.class)
        private byte[] encapsulation;

        private String profileName;
        private Map<String, String> metadata;
    }
}
