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
package org.neo4j.driver.internal.encryption.async;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import javax.crypto.SecretKey;
import org.neo4j.driver.encryption.KeyEncapsulationOptions;
import org.neo4j.driver.encryption.KeyEncapsulationResult;
import org.neo4j.driver.encryption.KeyEncapsulationService;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;

public record DelegatingAsyncKeyEncapsulationService(KeyEncapsulationService delegate, Executor executor)
        implements AsyncKeyEncapsulationService {
    public DelegatingAsyncKeyEncapsulationService {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(delegate.executor());
    }

    @Override
    public CompletionStage<KeyEncapsulationResult> encapsulateAsync(KeyEncapsulationOptions options) {
        return CompletableFuture.supplyAsync(() -> delegate.encapsulate(options), executor);
    }

    @Override
    public CompletionStage<SecretKey> decapsulateAsync(byte[] encapsulation, Map<String, String> metadata) {
        return CompletableFuture.supplyAsync(() -> delegate.decapsulate(encapsulation, metadata), executor);
    }
}
