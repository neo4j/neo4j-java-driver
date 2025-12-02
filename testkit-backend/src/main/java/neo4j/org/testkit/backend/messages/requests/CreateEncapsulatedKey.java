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

import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.EncapsulatedKey;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
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

        return EncapsulatedKey.builder()
                .data(EncapsulatedKey.EncapsulatedKeyBody.builder()
                        .id(key.id())
                        .alias(key.alias().orElse(null))
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
    public static class CreateEncapsulatedKeyBody {
        private String driverId;
        private String alias;
        private String profileName;
    }
}
