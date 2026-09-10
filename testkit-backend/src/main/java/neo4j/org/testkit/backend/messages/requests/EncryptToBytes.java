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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.deserializer.HexByteArrayDeserializer;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherValueDeserializer;
import neo4j.org.testkit.backend.messages.responses.EncryptedValue;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.property_encryption.InternalPropertyEncryptionRequest;
import org.neo4j.driver.property_encryption.BasePropertyEncryption;
import org.neo4j.driver.property_encryption.PropertyEncryptionRequest;
import org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption;
import org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class EncryptToBytes implements TestkitRequest {
    private EncryptToBytesBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption();
        var request = propertyEncryptRequest(encryption, data);
        var encrypted = encryption.encryptToBytes(request);
        return createResponse(encrypted);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption(AsyncPropertyEncryption.class);
        var request = propertyEncryptRequest(encryption, data);
        return encryption.encryptToBytesAsync(request).thenApply(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption(ReactivePropertyEncryption.class);
        var request = propertyEncryptRequest(encryption, data);
        return Mono.fromDirect(flowPublisherToFlux(encryption.encryptToBytes(request)))
                .map(this::createResponse);
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption(
                org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption.class);
        var request = propertyEncryptRequest(encryption, data);
        return Mono.fromDirect(encryption.encryptToBytes(request)).map(this::createResponse);
    }

    private PropertyEncryptionRequest propertyEncryptRequest(
            BasePropertyEncryption encryption, EncryptToBytesBody data) {
        PropertyEncryptionRequest request;
        var value = Optional.ofNullable(data.getValue()).orElse(Values.NULL);
        var aadStep = PropertyEncryptionRequest.builder().fromValue(value);

        var profileStep = data.getAad() != null ? aadStep.withAAD(data.getAad()) : aadStep;
        var buildStep = data.getProfileName() != null ? profileStep.usingProfile(data.getProfileName()) : profileStep;
        if (data.getKeyAlias() != null) {
            request = buildStep.usingKeyAlias(data.getKeyAlias()).build();
        } else if (data.getKeyId() != null) {
            request = buildStep.usingKeyId(data.getKeyId()).build();
        } else {
            throw new IllegalStateException("No key or key alias provided");
        }

        if (data.getIv() != null) {
            var iv = data.getIv();
            try {
                var field = InternalPropertyEncryptionRequest.class.getDeclaredField("iv");
                field.setAccessible(true);
                field.set(request, iv);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        return request;
    }

    private EncryptedValue createResponse(byte[] encrypted) {
        return EncryptedValue.builder()
                .data(EncryptedValue.EncryptedValueBody.builder()
                        .encryptedBytes(encrypted)
                        .build())
                .build();
    }

    @Setter
    @Getter
    public static class EncryptToBytesBody {
        private String driverId;

        @JsonDeserialize(using = TestkitCypherValueDeserializer.class)
        private Value value;

        @JsonDeserialize(using = TestkitCypherValueDeserializer.class)
        private Value aad;

        private String profileName;
        private String keyAlias;
        private String keyId;

        @JsonDeserialize(using = HexByteArrayDeserializer.class)
        private byte[] iv;
    }
}
