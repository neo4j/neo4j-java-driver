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
package org.neo4j.driver.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.MockitoAnnotations.openMocks;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.neo4j.bolt.connection.codec.WriteOutputs;
import org.neo4j.bolt.connection.codec.packstream.struct.EncryptedStructureEncoder;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncoder;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncodingSchemeVersion;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.encryption.BasePropertyEncryption;
import org.neo4j.driver.encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.encryption.EnvelopePropertyEncryptionProfile;
import org.neo4j.driver.encryption.PropertyDecryptionRequest;
import org.neo4j.driver.encryption.PropertyEncryptionRequest;
import org.neo4j.driver.encryption.async.AsyncEncapsulatedKeyRecordRepository;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.PropertyEncryptionException;
import org.neo4j.driver.internal.encryption.AEADEncryptedProperty;
import org.neo4j.driver.internal.encryption.PackStreamEncoderFactoryLoader;
import org.neo4j.driver.types.TypeSystem;

public abstract class AbstractPropertyEncryptionIT<T extends BasePropertyEncryption> {
    @Mock
    AsyncKeyEncapsulationService keyEncapsulationService;

    @Mock
    AsyncEncapsulatedKeyRecordRepository keyRepository;

    Driver driver;

    protected T propertyEncryption;

    String keyId;

    byte[] encryptedBytes;

    @SuppressWarnings("resource")
    @BeforeEach
    void beforeEach() throws IOException {
        openMocks(this);
        var mainProfile = EnvelopePropertyEncryptionProfile.builder("main", keyEncapsulationService, keyRepository)
                .build();
        var config =
                Config.builder().withPropertyEncryptionProfiles(mainProfile).build();
        driver = GraphDatabase.driver("bolt://localhost:7687", config);
        propertyEncryption = driver.propertyEncryption(propertyEncryptionType());
        keyId = UUID.randomUUID().toString();
        encryptedBytes = createEncryptedBytes(createEncryptedProperty(
                keyId, "NULL", ValueEncodingSchemeVersion.V1_0, ValueEncodingSchemeVersion.V1_0));
    }

    private static byte[] createEncryptedBytes(AEADEncryptedProperty encryptedProperty) throws IOException {
        @SuppressWarnings("deprecation")
        var factory = new PackStreamEncoderFactoryLoader(Logging.none()).factory();
        var encoder = factory.create(Set.of(EncryptedStructureEncoder.getInstance()));
        var writeOutput = WriteOutputs.bytes();
        encoder.encode(encryptedProperty.toEncryptedStruct(), writeOutput);
        var bytesOutput = writeOutput.output();
        var encryptedBytes = new byte[bytesOutput.length + 1];
        encryptedBytes[0] = 1;
        System.arraycopy(bytesOutput, 0, encryptedBytes, 1, bytesOutput.length);
        return encryptedBytes;
    }

    private static AEADEncryptedProperty createEncryptedProperty(
            String keyId,
            String typeName,
            ValueEncodingSchemeVersion typeBaseVersion,
            ValueEncodingSchemeVersion aadBaseVersion) {
        return new AEADEncryptedProperty(
                "ENVELOPE",
                1,
                "main",
                new byte[100],
                new byte[100],
                new ValueEncoder.Encoded(new byte[100], aadBaseVersion),
                keyId,
                typeName,
                typeBaseVersion);
    }

    @AfterEach
    void afterEach() {
        if (driver != null) {
            driver.close();
        }
    }

    @ParameterizedTest
    @EnumSource
    void shouldPassThroughNeo4jExceptionFromEncapsulationServiceOnKeyCreate(FailureMode failureMode) {
        // GIVEN
        var encapsulateException = new Neo4jException("encapsulate exception");
        switch (failureMode) {
            case IMMEDIATE ->
                given(keyEncapsulationService.encapsulateAsync(any())).willThrow(encapsulateException);
            case ASYNC ->
                given(keyEncapsulationService.encapsulateAsync(any()))
                        .willReturn(CompletableFuture.failedStage(encapsulateException));
        }

        // WHEN & THEN
        var error = assertThrows(Neo4jException.class, this::createKey);
        assertEquals(encapsulateException, error);
    }

    @ParameterizedTest
    @EnumSource
    void shouldWrapNonNeo4jExceptionFromEncapsulationServiceOnKeyCreate(FailureMode failureMode) {
        // GIVEN
        var encapsulateException = new IllegalStateException("decapsulate exception");
        switch (failureMode) {
            case IMMEDIATE ->
                given(keyEncapsulationService.encapsulateAsync(any())).willThrow(encapsulateException);
            case ASYNC ->
                given(keyEncapsulationService.encapsulateAsync(any()))
                        .willReturn(CompletableFuture.failedStage(encapsulateException));
        }

        // WHEN & THEN
        var error = assertThrows(PropertyEncryptionException.class, this::createKey);
        assertEquals(encapsulateException, error.getCause());
    }

    @ParameterizedTest
    @MethodSource("keyReferenceTypesAndFailureModes")
    void shouldPassThroughNeo4jExceptionFromEncapsulationServiceOnEncryptToBytes(
            KeyReferenceType keyReferenceType, FailureMode failureMode) {
        // GIVEN
        var reference = "reference";
        var encapsulateException = new Neo4jException("decapsulate exception");
        var encapsulation = new byte[100];
        var metadata = Map.<String, String>of();
        switch (failureMode) {
            case IMMEDIATE ->
                given(keyEncapsulationService.decapsulateAsync(encapsulation, metadata))
                        .willThrow(encapsulateException);
            case ASYNC ->
                given(keyEncapsulationService.decapsulateAsync(encapsulation, metadata))
                        .willReturn(CompletableFuture.failedStage(encapsulateException));
        }
        var encapsulatedKeyRecord = EncapsulatedKeyRecord.of(reference, reference, encapsulation, metadata);
        switch (keyReferenceType) {
            case ID ->
                given(keyRepository.findByIdAsync(reference))
                        .willReturn(CompletableFuture.completedStage(encapsulatedKeyRecord));
            case ALIAS ->
                given(keyRepository.findByAliasAsync(reference))
                        .willReturn(CompletableFuture.completedStage(encapsulatedKeyRecord));
        }
        var plaintextValue = "plaintext";
        var nextStep = PropertyEncryptionRequest.builder().fromValue(plaintextValue);
        var encryptionRequest =
                switch (keyReferenceType) {
                    case ID -> nextStep.usingKeyId(reference).build();
                    case ALIAS -> nextStep.usingKeyAlias(reference).build();
                };

        // WHEN & THEN
        var error = assertThrows(Neo4jException.class, () -> encryptToBytes(encryptionRequest));
        assertEquals(encapsulateException, error);
    }

    @ParameterizedTest
    @MethodSource("keyReferenceTypesAndFailureModes")
    void shouldWrapNonNeo4jExceptionFromEncapsulationServiceOnEncryptToBytes(
            KeyReferenceType keyReferenceType, FailureMode failureMode) {
        // GIVEN
        var reference = "reference";
        var encapsulateException = new IllegalStateException("decapsulate exception");
        var encapsulation = new byte[100];
        var metadata = Map.<String, String>of();
        switch (failureMode) {
            case IMMEDIATE ->
                given(keyEncapsulationService.decapsulateAsync(encapsulation, metadata))
                        .willThrow(encapsulateException);
            case ASYNC ->
                given(keyEncapsulationService.decapsulateAsync(encapsulation, metadata))
                        .willReturn(CompletableFuture.failedStage(encapsulateException));
        }
        var encapsulatedKeyRecord = EncapsulatedKeyRecord.of(reference, reference, encapsulation, metadata);
        switch (keyReferenceType) {
            case ID ->
                given(keyRepository.findByIdAsync(reference))
                        .willReturn(CompletableFuture.completedStage(encapsulatedKeyRecord));
            case ALIAS ->
                given(keyRepository.findByAliasAsync(reference))
                        .willReturn(CompletableFuture.completedStage(encapsulatedKeyRecord));
        }
        var mainProfile = EnvelopePropertyEncryptionProfile.builder("main", keyEncapsulationService, keyRepository)
                .build();
        var config =
                Config.builder().withPropertyEncryptionProfiles(mainProfile).build();
        var driver = GraphDatabase.driver("bolt://localhost:7687", config);
        var propertyEncryption = driver.propertyEncryption();
        var plaintextValue = "plaintext";
        var nextStep = PropertyEncryptionRequest.builder().fromValue(plaintextValue);
        var encryptionRequest =
                switch (keyReferenceType) {
                    case ID -> nextStep.usingKeyId(reference).build();
                    case ALIAS -> nextStep.usingKeyAlias(reference).build();
                };

        // WHEN & THEN
        var error = assertThrows(PropertyEncryptionException.class, () -> encryptToBytes(encryptionRequest));
        assertEquals(encapsulateException, error.getCause());
    }

    @ParameterizedTest
    @EnumSource
    void shouldPassThroughNeo4jExceptionFromEncapsulationServiceOnDecrypt(FailureMode failureMode) {
        // GIVEN
        var encapsulateException = new Neo4jException("decapsulate exception");
        var encapsulation = new byte[100];
        var metadata = Map.<String, String>of();
        switch (failureMode) {
            case IMMEDIATE ->
                given(keyEncapsulationService.decapsulateAsync(encapsulation, metadata))
                        .willThrow(encapsulateException);
            case ASYNC ->
                given(keyEncapsulationService.decapsulateAsync(encapsulation, metadata))
                        .willReturn(CompletableFuture.failedStage(encapsulateException));
        }
        var encapsulatedKeyRecord = EncapsulatedKeyRecord.of(keyId, null, encapsulation, metadata);
        given(keyRepository.findByIdAsync(keyId)).willReturn(CompletableFuture.completedStage(encapsulatedKeyRecord));
        var decryptionRequest = PropertyDecryptionRequest.builder()
                .fromValue(encryptedBytes)
                .withoutExternalAAD()
                .build();

        // WHEN & THEN
        var error = assertThrows(Neo4jException.class, () -> decrypt(decryptionRequest));
        assertEquals(encapsulateException, error);
    }

    @ParameterizedTest
    @EnumSource
    void shouldWrapNonNeo4jExceptionFromEncapsulationServiceOnDecrypt(FailureMode failureMode) {
        // GIVEN
        var encapsulateException = new IllegalStateException("decapsulate exception");
        var encapsulation = new byte[100];
        var metadata = Map.<String, String>of();
        switch (failureMode) {
            case IMMEDIATE ->
                given(keyEncapsulationService.decapsulateAsync(encapsulation, metadata))
                        .willThrow(encapsulateException);
            case ASYNC ->
                given(keyEncapsulationService.decapsulateAsync(encapsulation, metadata))
                        .willReturn(CompletableFuture.failedStage(encapsulateException));
        }
        var encapsulatedKeyRecord = EncapsulatedKeyRecord.of(keyId, null, encapsulation, metadata);
        given(keyRepository.findByIdAsync(keyId)).willReturn(CompletableFuture.completedStage(encapsulatedKeyRecord));
        var decryptionRequest = PropertyDecryptionRequest.builder()
                .fromValue(encryptedBytes)
                .withoutExternalAAD()
                .build();

        // WHEN & THEN
        var error = assertThrows(PropertyEncryptionException.class, () -> decrypt(decryptionRequest));
        assertEquals(encapsulateException, error.getCause());
    }

    @ParameterizedTest
    @MethodSource("propertiesWithNewerTypeVersion")
    void shouldReturnUnsupportedTypeForNewerTypeEncodingVersions(AEADEncryptedProperty encryptedProperty)
            throws IOException {
        // GIVEN
        var encryptedBytes = createEncryptedBytes(encryptedProperty);
        var decryptionRequest = PropertyDecryptionRequest.builder()
                .fromValue(encryptedBytes)
                .withoutExternalAAD()
                .build();

        // WHEN
        var value = decrypt(decryptionRequest);

        // THEN
        assertTrue(TypeSystem.getDefault().UNSUPPORTED().isTypeOf(value));
        var unsupported = value.asUnsupportedType();
        assertEquals(encryptedProperty.typeName(), unsupported.name());
        assertEquals(
                "%d.%d"
                        .formatted(
                                encryptedProperty.typeBaseVersion().majorVersion(),
                                encryptedProperty.typeBaseVersion().minorVersion()),
                unsupported.minProtocolVersion());
        assertFalse(unsupported.message().orElseThrow().isEmpty());
    }

    @ParameterizedTest
    @MethodSource("propertiesWithNewerAADVersion")
    void shouldThrowOnNewerAADEncodingVersions(AEADEncryptedProperty encryptedProperty) throws IOException {
        // GIVEN
        var encryptedBytes = createEncryptedBytes(encryptedProperty);
        var decryptionRequest = PropertyDecryptionRequest.builder()
                .fromValue(encryptedBytes)
                .withAAD("shouldThrowBeforeThis")
                .build();

        // WHEN & THEN
        var error = assertThrows(PropertyEncryptionException.class, () -> decrypt(decryptionRequest));
        assertTrue(error.getMessage()
                .contains("%d.%d"
                        .formatted(
                                encryptedProperty.encodedAad().baseVersion().majorVersion(),
                                encryptedProperty.encodedAad().baseVersion().minorVersion())));
    }

    protected abstract Class<T> propertyEncryptionType();

    protected abstract byte[] encryptToBytes(PropertyEncryptionRequest request);

    protected abstract Value decrypt(PropertyDecryptionRequest request);

    protected abstract void createKey();

    private static Stream<Arguments> keyReferenceTypesAndFailureModes() {
        return Stream.of(
                Arguments.of(KeyReferenceType.ID, FailureMode.IMMEDIATE),
                Arguments.of(KeyReferenceType.ID, FailureMode.ASYNC),
                Arguments.of(KeyReferenceType.ALIAS, FailureMode.IMMEDIATE),
                Arguments.of(KeyReferenceType.ALIAS, FailureMode.ASYNC));
    }

    private static Stream<Arguments> propertiesWithNewerTypeVersion() {
        return Stream.of(
                Arguments.of(createEncryptedProperty(
                        "keyId", "FUTURE_TYPE", new ValueEncodingSchemeVersion(1, 1), ValueEncodingSchemeVersion.V1_0)),
                Arguments.of(createEncryptedProperty(
                        "keyId",
                        "FUTURE_TYPE",
                        new ValueEncodingSchemeVersion(2, 0),
                        ValueEncodingSchemeVersion.V1_0)));
    }

    private static Stream<Arguments> propertiesWithNewerAADVersion() {
        return Stream.of(
                Arguments.of(createEncryptedProperty(
                        "keyId", "FUTURE_TYPE", ValueEncodingSchemeVersion.V1_0, new ValueEncodingSchemeVersion(1, 1))),
                Arguments.of(createEncryptedProperty(
                        "keyId",
                        "FUTURE_TYPE",
                        ValueEncodingSchemeVersion.V1_0,
                        new ValueEncodingSchemeVersion(2, 0))));
    }

    enum KeyReferenceType {
        ID,
        ALIAS
    }

    enum FailureMode {
        IMMEDIATE,
        ASYNC
    }
}
