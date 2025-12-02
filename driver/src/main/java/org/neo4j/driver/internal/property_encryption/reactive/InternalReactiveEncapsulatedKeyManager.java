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
package org.neo4j.driver.internal.property_encryption.reactive;

import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

import java.util.concurrent.Flow;
import org.neo4j.driver.internal.property_encryption.AbstractEncapsulatedKeyManager;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.PropertyEncryptionProfile;
import org.neo4j.driver.property_encryption.reactive.ReactiveEncapsulatedKeyManager;
import reactor.core.publisher.Mono;

final class InternalReactiveEncapsulatedKeyManager extends AbstractEncapsulatedKeyManager
        implements ReactiveEncapsulatedKeyManager {
    public InternalReactiveEncapsulatedKeyManager(
            KeyEncapsulationService keyEncapsulationService,
            PropertyEncryptionProfile.Envelope.EncapsulatedKeyRepository keyRepository) {
        super(keyEncapsulationService, keyRepository);
    }

    @Override
    public Flow.Publisher<EncapsulatedKey> create(String alias, KeyEncapsulationOptions encapsulationOptions) {
        return publisherToFlowPublisher(Mono.fromCompletionStage(() -> createAsync(alias, encapsulationOptions)));
    }

    @Override
    public Flow.Publisher<EncapsulatedKey> findByAlias(String alias) {
        return publisherToFlowPublisher(Mono.fromCompletionStage(() -> findByAliasAsync(alias)));
    }

    @Override
    public Flow.Publisher<Void> updateAliasById(String id, String alias) {
        return publisherToFlowPublisher(Mono.fromCompletionStage(() -> updateAliasByIdAsync(id, alias)));
    }

    @Override
    public Flow.Publisher<Void> deleteById(String id) {
        return publisherToFlowPublisher(Mono.fromCompletionStage(() -> deleteByIdAsync(id)));
    }
}
