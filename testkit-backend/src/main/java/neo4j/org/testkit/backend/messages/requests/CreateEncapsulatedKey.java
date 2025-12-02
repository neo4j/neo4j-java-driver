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

import static reactor.adapter.JdkFlowAdapter.flowPublisherToFlux;

import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.property_encryption.EncapsulatedKey;
import org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption;
import org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class CreateEncapsulatedKey implements TestkitRequest {
    private CreateEncapsulatedKeyBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption();
        var keyManager = encryption.keyManager(data.getProfileName());
        var key = keyManager.create(data.getAlias());
        return createResponse(key);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption(AsyncPropertyEncryption.class);
        var keyManager = encryption.keyManager(data.getProfileName());
        return keyManager.createAsync(data.getAlias()).thenApply(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption(ReactivePropertyEncryption.class);
        var keyManager = encryption.keyManager(data.getProfileName());
        return Mono.fromDirect(flowPublisherToFlux(keyManager.create(data.getAlias())))
                .map(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption(
                org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption.class);
        var keyManager = encryption.keyManager(data.getProfileName());
        return Mono.fromDirect(keyManager.create(data.getAlias())).map(this::createResponse);
    }

    private neo4j.org.testkit.backend.messages.responses.EncapsulatedKey createResponse(EncapsulatedKey key) {
        return neo4j.org.testkit.backend.messages.responses.EncapsulatedKey.builder()
                .data(neo4j.org.testkit.backend.messages.responses.EncapsulatedKey.EncapsulatedKeyBody.builder()
                        .id(key.id())
                        .alias(key.alias().orElse(null))
                        .build())
                .build();
    }

    @Setter
    @Getter
    public static class CreateEncapsulatedKeyBody {
        private String driverId;
        private String alias;
        private String profileName;
    }
}
