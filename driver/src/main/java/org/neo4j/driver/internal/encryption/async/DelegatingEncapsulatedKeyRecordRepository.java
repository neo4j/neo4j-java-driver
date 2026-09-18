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
import org.neo4j.driver.encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.encryption.async.AsyncEncapsulatedKeyRecordRepository;

public record DelegatingEncapsulatedKeyRecordRepository(EncapsulatedKeyRecordRepository delegate, Executor executor)
        implements AsyncEncapsulatedKeyRecordRepository {
    public DelegatingEncapsulatedKeyRecordRepository {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(delegate.executor());
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> findByIdAsync(String id) {
        return CompletableFuture.supplyAsync(() -> delegate.findById(id).orElse(null), executor);
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> findByAliasAsync(String alias) {
        return CompletableFuture.supplyAsync(() -> delegate.findByAlias(alias).orElse(null), executor);
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> createAsync(
            String alias, byte[] encapsulation, Map<String, String> metadata) {
        return CompletableFuture.supplyAsync(() -> delegate.create(alias, encapsulation, metadata), executor);
    }

    @Override
    public CompletionStage<Void> setAliasByIdAsync(String id, String alias) {
        return CompletableFuture.runAsync(() -> delegate.setAliasById(id, alias), executor);
    }

    @Override
    public CompletionStage<Void> deleteByIdAsync(String id) {
        return CompletableFuture.runAsync(() -> delegate.deleteById(id), executor);
    }
}
