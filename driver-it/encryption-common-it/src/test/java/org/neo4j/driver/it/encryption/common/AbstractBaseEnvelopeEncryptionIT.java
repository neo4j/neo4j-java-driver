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
package org.neo4j.driver.it.encryption.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Value;
import org.neo4j.driver.property_encryption.BasePropertyEncryption;
import org.neo4j.driver.property_encryption.EncapsulatedKey;
import org.neo4j.driver.property_encryption.EnvelopePropertyEncryptionProfile;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.PropertyDecryptionRequest;
import org.neo4j.driver.property_encryption.PropertyEncryptionRequest;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
abstract class AbstractBaseEnvelopeEncryptionIT<T extends BasePropertyEncryption> {
    @SuppressWarnings("resource")
    @Container
    private static final Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>(
                    DockerImageName.parse("neo4j:%s-enterprise"
                            .formatted(Optional.ofNullable(System.getenv("NEO4J_VERSION"))
                                    .orElse("2025.04.0"))))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes");

    KeyEncapsulationService keyEncapsulationService;
    String keyAlias = "main-key";
    Driver keyDriver;
    Driver driver;
    T encryption;

    @BeforeEach
    void beforeEach() throws IOException, NoSuchAlgorithmException {
        keyEncapsulationService = keyEncapsulationService();

        keyDriver = GraphDatabase.driver(
                neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword()));
        var keyRepository = new Neo4JKeyRecordRepository(keyDriver, "neo4j");
        keyRepository.createConstraints();

        var provider = provider();
        var builder = EnvelopePropertyEncryptionProfile.builder("reference", keyEncapsulationService, keyRepository);
        if (provider != null) {
            builder = builder.withCryptoContext(provider, secureRandomIV(provider));
        }
        var encryptionProfile = builder.build();
        var config = Config.builder()
                .withPropertyEncryptionProfiles(encryptionProfile)
                .build();
        driver = GraphDatabase.driver(
                neo4jContainer.getBoltUrl(), AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword()), config);
        encryption = driver.propertyEncryption(encryptionClass());
        createKey(keyAlias);
    }

    @Test
    void shouldUpdateAlias() {
        var originalAlias = "alias";
        var key = createKey(originalAlias);

        deleteAliasById(key.id());
        var updatedAlias = "updatedAlias";
        setAliasById(key.id(), updatedAlias);

        var updatedKey = findByAlias(updatedAlias).orElseThrow();

        assertEquals(key.id(), updatedKey.id());
        assertEquals(updatedAlias, updatedKey.alias().orElse(null));
    }

    @Test
    void shouldEncryptAndDecrypt() {
        var name = "username";
        var phone = "00000000";

        // WRITE TO DATABASE
        var encryptRequest = PropertyEncryptionRequest.builder()
                .fromValue(phone)
                .withAAD(name)
                .usingKeyAlias(keyAlias)
                .build();
        var encryptedPhone = encrypt(encryptRequest);
        var result = driver.executableQuery("CREATE (user:User {name: $name, phone: $phone}) RETURN user")
                .withParameters(Map.of("name", name, "phone", encryptedPhone))
                .execute();

        // READ ENCRYPTED DATA
        var user = result.records().get(0).get("user");
        var decryptRequest = PropertyDecryptionRequest.builder()
                .fromValue(user.get("phone").asByteArray())
                .withAAD(name)
                .build();
        var decryptedPhone = decrypt(decryptRequest).asString();

        assertEquals(phone, decryptedPhone);
    }

    @AfterEach
    void afterEach() {
        deleteKey();
        driver.close();
    }

    protected abstract KeyEncapsulationService keyEncapsulationService() throws IOException, NoSuchAlgorithmException;

    protected abstract Class<T> encryptionClass();

    protected abstract EncapsulatedKey createKey(String alias);

    protected abstract void setAliasById(String id, String alias);

    protected abstract void deleteAliasById(String id);

    protected abstract Optional<EncapsulatedKey> findByAlias(String alias);

    protected abstract void deleteKey();

    protected abstract byte[] encrypt(PropertyEncryptionRequest request);

    protected abstract Value decrypt(PropertyDecryptionRequest request);

    protected Provider provider() {
        return null;
    }

    protected SecureRandom secureRandomIV(Provider provider) throws NoSuchAlgorithmException {
        return null;
    }
}
