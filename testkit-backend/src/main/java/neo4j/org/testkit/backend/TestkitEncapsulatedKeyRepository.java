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
package neo4j.org.testkit.backend;

import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import neo4j.org.testkit.backend.messages.requests.EncapsulatedKeyRepositoryCreateCompleted;
import neo4j.org.testkit.backend.messages.requests.EncapsulatedKeyRepositoryDeleteCompleted;
import neo4j.org.testkit.backend.messages.requests.EncapsulatedKeyRepositoryErrorCompleted;
import neo4j.org.testkit.backend.messages.requests.EncapsulatedKeyRepositoryFindByAliasCompleted;
import neo4j.org.testkit.backend.messages.requests.EncapsulatedKeyRepositoryFindByIdCompleted;
import neo4j.org.testkit.backend.messages.requests.EncapsulatedKeyRepositorySetAliasCompleted;
import neo4j.org.testkit.backend.messages.requests.TestkitCallbackResult;
import neo4j.org.testkit.backend.messages.responses.EncapsulatedKeyRepositoryCreateRequest;
import neo4j.org.testkit.backend.messages.responses.EncapsulatedKeyRepositoryDeleteRequest;
import neo4j.org.testkit.backend.messages.responses.EncapsulatedKeyRepositoryFindByAliasRequest;
import neo4j.org.testkit.backend.messages.responses.EncapsulatedKeyRepositoryFindByIdRequest;
import neo4j.org.testkit.backend.messages.responses.EncapsulatedKeyRepositorySetAliasRequest;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import org.neo4j.driver.encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.encryption.async.AsyncEncapsulatedKeyRecordRepository;

public class TestkitEncapsulatedKeyRepository implements AsyncEncapsulatedKeyRecordRepository {
    private final String id;
    private final TestkitState testkitState;

    public TestkitEncapsulatedKeyRepository(String id, TestkitState testkitState) {
        this.id = Objects.requireNonNull(id);
        this.testkitState = Objects.requireNonNull(testkitState);
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> findByIdAsync(String id) {
        var callbackId = testkitState.newId();

        var callback = EncapsulatedKeyRepositoryFindByIdRequest.builder()
                .data(EncapsulatedKeyRepositoryFindByIdRequest.EncapsulatedKeyRepositoryFindByIdRequestBody.builder()
                        .id(callbackId)
                        .repositoryId(this.id)
                        .keyId(id)
                        .build())
                .build();

        var callbackStage = dispatchTestkitCallback(testkitState, callback);
        TestkitCallbackResult result;
        try {
            result = callbackStage.toCompletableFuture().get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected failure during Testkit callback", e);
        }

        if (result instanceof EncapsulatedKeyRepositoryErrorCompleted errorCompleted) {
            return CompletableFuture.failedStage(new CustomDriverError(
                    new RuntimeException(errorCompleted.getData().getDetail())));
        } else if (result instanceof EncapsulatedKeyRepositoryFindByIdCompleted findByIdCompleted) {
            return CompletableFuture.completedFuture(
                    parseRecord(findByIdCompleted.getData().getRecord()));
        } else {
            throw new RuntimeException("Unexpected failure during Testkit callback");
        }
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> findByAliasAsync(String alias) {
        var callbackId = testkitState.newId();

        var callback = EncapsulatedKeyRepositoryFindByAliasRequest.builder()
                .data(
                        EncapsulatedKeyRepositoryFindByAliasRequest.EncapsulatedKeyRepositoryFindByAliasRequestBody
                                .builder()
                                .id(callbackId)
                                .repositoryId(this.id)
                                .alias(alias)
                                .build())
                .build();

        var callbackStage = dispatchTestkitCallback(testkitState, callback);
        TestkitCallbackResult result;
        try {
            result = callbackStage.toCompletableFuture().get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected failure during Testkit callback", e);
        }

        if (result instanceof EncapsulatedKeyRepositoryErrorCompleted errorCompleted) {
            return CompletableFuture.failedStage(new CustomDriverError(
                    new RuntimeException(errorCompleted.getData().getDetail())));
        } else if (result instanceof EncapsulatedKeyRepositoryFindByAliasCompleted findByAliasCompleted) {
            return CompletableFuture.completedFuture(
                    parseRecord(findByAliasCompleted.getData().getRecord()));
        } else {
            throw new RuntimeException("Unexpected failure during Testkit callback");
        }
    }

    @Override
    public CompletionStage<EncapsulatedKeyRecord> createAsync(
            String alias, byte[] encapsulation, Map<String, String> metadata) {
        var callbackId = testkitState.newId();

        var callback = EncapsulatedKeyRepositoryCreateRequest.builder()
                .data(EncapsulatedKeyRepositoryCreateRequest.EncapsulatedKeyRepositoryCreateRequestBody.builder()
                        .id(callbackId)
                        .repositoryId(id)
                        .alias(alias)
                        .encapsulation(encapsulation)
                        .metadata(metadata)
                        .build())
                .build();

        var callbackStage = dispatchTestkitCallback(testkitState, callback);
        TestkitCallbackResult result;
        try {
            result = callbackStage.toCompletableFuture().get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected failure during Testkit callback", e);
        }

        if (result instanceof EncapsulatedKeyRepositoryErrorCompleted errorCompleted) {
            return CompletableFuture.failedStage(new CustomDriverError(
                    new RuntimeException(errorCompleted.getData().getDetail())));
        } else if (result instanceof EncapsulatedKeyRepositoryCreateCompleted createCompleted) {
            return CompletableFuture.completedFuture(
                    parseRecord(createCompleted.getData().getRecord()));
        } else {
            throw new RuntimeException("Unexpected failure during Testkit callback");
        }
    }

    @Override
    public CompletionStage<Void> setAliasByIdAsync(String id, String alias) {
        var callbackId = testkitState.newId();

        var callback = EncapsulatedKeyRepositorySetAliasRequest.builder()
                .data(EncapsulatedKeyRepositorySetAliasRequest.EncapsulatedKeyRepositorySetAliasRequestBody.builder()
                        .id(callbackId)
                        .repositoryId(this.id)
                        .keyId(id)
                        .alias(alias)
                        .build())
                .build();

        var callbackStage = dispatchTestkitCallback(testkitState, callback);
        TestkitCallbackResult result;
        try {
            result = callbackStage.toCompletableFuture().get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected failure during Testkit callback", e);
        }

        if (result instanceof EncapsulatedKeyRepositoryErrorCompleted errorCompleted) {
            return CompletableFuture.failedStage(new CustomDriverError(
                    new RuntimeException(errorCompleted.getData().getDetail())));
        } else if (result instanceof EncapsulatedKeyRepositorySetAliasCompleted setAliasCompleted) {
            return CompletableFuture.completedFuture(null);
        } else {
            throw new RuntimeException("Unexpected failure during Testkit callback");
        }
    }

    @Override
    public CompletionStage<Void> deleteByIdAsync(String id) {
        var callbackId = testkitState.newId();

        var callback = EncapsulatedKeyRepositoryDeleteRequest.builder()
                .data(EncapsulatedKeyRepositoryDeleteRequest.EncapsulatedKeyRepositoryDeleteRequestBody.builder()
                        .id(callbackId)
                        .repositoryId(this.id)
                        .keyId(id)
                        .build())
                .build();

        var callbackStage = dispatchTestkitCallback(testkitState, callback);
        TestkitCallbackResult result;
        try {
            result = callbackStage.toCompletableFuture().get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected failure during Testkit callback", e);
        }

        if (result instanceof EncapsulatedKeyRepositoryErrorCompleted errorCompleted) {
            return CompletableFuture.failedStage(new CustomDriverError(
                    new RuntimeException(errorCompleted.getData().getDetail())));
        } else if (result instanceof EncapsulatedKeyRepositoryDeleteCompleted deleteCompleted) {
            return CompletableFuture.completedFuture(null);
        } else {
            throw new RuntimeException("Unexpected failure during Testkit callback");
        }
    }

    public String id() {
        return id;
    }

    private CompletionStage<TestkitCallbackResult> dispatchTestkitCallback(
            TestkitState testkitState, TestkitCallback response) {
        var future = new CompletableFuture<TestkitCallbackResult>();
        testkitState.getCallbackIdToFuture().put(response.getCallbackId(), future);
        testkitState.getResponseWriter().accept(response);
        return future;
    }

    private EncapsulatedKeyRecord parseRecord(
            neo4j.org.testkit.backend.messages.requests.EncapsulatedKeyRecord record) {

        var value = record.getEncapsulation();
        byte[] bytes;

        try {
            bytes = Base64.getDecoder().decode(value);
        } catch (Exception e) {
            bytes = java.util.HexFormat.of().parseHex(record.getEncapsulation().replace(" ", ""));
        }

        return EncapsulatedKeyRecord.of(record.getId(), record.getAlias(), bytes, record.getMetadata());
    }
}
