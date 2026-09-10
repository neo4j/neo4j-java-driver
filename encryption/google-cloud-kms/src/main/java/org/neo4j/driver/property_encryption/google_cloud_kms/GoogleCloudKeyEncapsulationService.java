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
package org.neo4j.driver.property_encryption.google_cloud_kms;

import com.google.api.core.ApiFuture;
import com.google.cloud.kms.v1.DecryptRequest;
import com.google.cloud.kms.v1.EncryptRequest;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationResult;
import org.neo4j.driver.property_encryption.KeyEncapsulationResults;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;

public final class GoogleCloudKeyEncapsulationService implements KeyEncapsulationService {
    private final KeyManagementServiceClient keyManagementServiceClient;
    private final CloudKmsKeyEncapsulationOptions defaultOptions;
    private final KeyGenerator keyGenerator;

    public GoogleCloudKeyEncapsulationService(CloudKmsKeyEncapsulationOptions defaultOptions)
            throws IOException, NoSuchAlgorithmException {
        this.keyManagementServiceClient = KeyManagementServiceClient.create();
        this.defaultOptions = Objects.requireNonNull(defaultOptions);
        this.keyGenerator = KeyGenerator.getInstance("AES");
        this.keyGenerator.init(256);
    }

    @Override
    public CompletionStage<KeyEncapsulationResult> encapsulate(KeyEncapsulationOptions options) {
        var encapsulationOptions = Objects.requireNonNullElse(options, defaultOptions);
        if (encapsulationOptions instanceof CloudKmsKeyEncapsulationOptions cloudOptions) {
            var key = keyGenerator.generateKey();
            var req = EncryptRequest.newBuilder()
                    .setName(cloudOptions.keyName().toString())
                    .setPlaintext(ByteString.copyFrom(key.getEncoded()))
                    .build();
            return toCompletionStage(
                            keyManagementServiceClient.encryptCallable().futureCall(req))
                    .thenApply(resp -> KeyEncapsulationResults.create(
                            resp.getCiphertext().toByteArray(), encapsulationOptions.toMap(), key));
        } else {
            return CompletableFuture.failedStage(new ClientException("Unsupported options"));
        }
    }

    @Override
    public CompletionStage<SecretKey> decapsulate(byte[] encapsulation, Map<String, String> metadata) {
        if (metadata == null) {
            return CompletableFuture.failedStage(new ClientException("Missing options"));
        }
        var cloudOptions = CloudKmsKeyEncapsulationOptions.of(metadata);
        var keyName = cloudOptions.keyName();
        var request = DecryptRequest.newBuilder()
                .setName(keyName.toString())
                .setCiphertext(ByteString.copyFrom(encapsulation))
                .build();

        return toCompletionStage(keyManagementServiceClient.decryptCallable().futureCall(request))
                .thenApply(resp -> new SecretKeySpec(resp.getPlaintext().toByteArray(), "AES"));
    }

    private record EnvelopeCiphertext(byte[] encryptedDek, byte[] iv, byte[] ciphertext, byte[] tag) {}

    private static <T> CompletionStage<T> toCompletionStage(ApiFuture<T> apiFuture) {
        var cf = new CompletableFuture<T>();
        apiFuture.addListener(
                () -> {
                    try {
                        cf.complete(apiFuture.get());
                    } catch (Exception e) {
                        cf.completeExceptionally(e);
                    }
                },
                MoreExecutors.directExecutor());
        return cf;
    }
}
