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
package org.neo4j.driver.property_encryption.aws_kms;

import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.neo4j.driver.property_encryption.KeyEncapsulationOptions;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsAsyncClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptRequest;

public final class AWSKeyEncapsulationService implements KeyEncapsulationService {
    private final KmsAsyncClient kms;
    private final AwsKeyEncapsulationOptions defaultOptions;
    private final KeyGenerator keyGenerator;

    public AWSKeyEncapsulationService(AwsKeyEncapsulationOptions defaultOptions) throws NoSuchAlgorithmException {
        this.kms = KmsAsyncClient.create();
        this.defaultOptions = Objects.requireNonNull(defaultOptions);
        this.keyGenerator = KeyGenerator.getInstance("AES");
        this.keyGenerator.init(256);
    }

    @Override
    public CompletionStage<EncapsulationResult> encapsulate(KeyEncapsulationOptions options) {
        var encapsulationOptions = Objects.requireNonNullElse(options, defaultOptions);
        var kmsKeyId = ((AwsKeyEncapsulationOptions) encapsulationOptions).keyId();
        var key = keyGenerator.generateKey();
        var req = EncryptRequest.builder()
                .keyId(kmsKeyId)
                .plaintext(SdkBytes.fromByteArray(key.getEncoded()))
                .build();
        return kms.encrypt(req)
                .thenApply(encryptResponse -> EncapsulationResult.of(
                        encryptResponse.ciphertextBlob().asByteArray(), encapsulationOptions, key));
    }

    @Override
    public CompletionStage<SecretKey> decapsulate(byte[] ciphertext, Map<String, String> metadata) {
        var options = AwsKeyEncapsulationOptions.of(metadata);
        var req = DecryptRequest.builder()
                .keyId(options.keyId())
                .ciphertextBlob(SdkBytes.fromByteArray(ciphertext))
                .build();
        return kms.decrypt(req)
                .thenApply(decryptResponse ->
                        new SecretKeySpec(decryptResponse.plaintext().asByteArray(), "AES"));
    }
}
