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
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.requests.deserializer.HexByteArrayDeserializer;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherValueDeserializer;
import neo4j.org.testkit.backend.messages.responses.DecryptedValue;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Value;
import org.neo4j.driver.property_encryption.PropertyDecryptRequest;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class Decrypt implements TestkitRequest {
    private DecryptBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        @SuppressWarnings("resource")
        var driver = testkitState.getDriverHolder(data.getDriverId()).driver();
        var encryption = driver.propertyEncryption();
        PropertyDecryptRequest request;

        var aadStep = encryption.decryptRequest().fromValue(data.getValue());

        var persistAADStep = data.isUsePersistedAad() ? aadStep.withPersistedAAD() : aadStep.withAAD(data.getAad());

        request = persistAADStep.build();
        var decrypted = encryption.decrypt(request);
        return DecryptedValue.builder()
                .data(DecryptedValue.EncryptedValueBody.builder()
                        .decryptedValue(decrypted)
                        .build())
                .build();
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
    public static class DecryptBody {
        private String driverId;

        @JsonDeserialize(using = HexByteArrayDeserializer.class)
        private byte[] value;

        @JsonDeserialize(using = TestkitCypherValueDeserializer.class)
        private Value aad;

        private boolean usePersistedAad;
    }
}
