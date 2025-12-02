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
package org.neo4j.driver.internal.property_encryption;

import static javax.crypto.Cipher.DECRYPT_MODE;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import org.neo4j.bolt.connection.codec.ReadInputs;
import org.neo4j.bolt.connection.codec.WriteOutputs;
import org.neo4j.bolt.connection.codec.packstream.struct.EncryptedStructure;
import org.neo4j.bolt.connection.codec.packstream.struct.EncryptedStructureDecoder;
import org.neo4j.bolt.connection.codec.packstream.struct.EncryptedStructureEncoder;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncoder;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncodingSchemeVersion;
import org.neo4j.bolt.connection.values.ValueFactory;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.types.TypeSystem;

final class AEADEncryption {
    private final String ENCRYPTION_PURPOSE = "neo4j/property-encryption/v1";
    private final Value EMPTY_AAD = Values.NULL;
    private final ValueFactory valueFactory;

    @SuppressWarnings("deprecation")
    private final Logging logging;

    AEADEncryption(ValueFactory valueFactory, @SuppressWarnings("deprecation") Logging logging) {
        this.valueFactory = Objects.requireNonNull(valueFactory);
        this.logging = Objects.requireNonNull(logging);
    }

    AEADEncryptedProperty encrypt(
            InternalPropertyEncryptRequest encryptionRequest, SecretKey key, String keyId, String profileName)
            throws NoSuchAlgorithmException, IllegalBlockSizeException, BadPaddingException,
                    InvalidAlgorithmParameterException, NoSuchPaddingException, InvalidKeyException {
        var plaintextValue = encryptionRequest.value;
        var encodedPlaintext = packValue(plaintextValue);
        var encodedVersion = encodedPlaintext.baseVersion();
        var iv = encryptionRequest.iv != null ? encryptionRequest.iv : generateIV();
        var gcmParameterSpec = new GCMParameterSpec(128, iv);
        ValueEncoder.Encoded encodedAad = encryptionRequest.aad != null ? packValue(encryptionRequest.aad) : null;
        // TODO decide if HKDF should be kept
        var encryptionKey = Hkdf.deriveAesKey(key, ENCRYPTION_PURPOSE);
        var cipher = prepareCipher(
                Cipher.ENCRYPT_MODE,
                encryptionKey,
                gcmParameterSpec,
                encodedAad != null ? encodedAad.bytes() : new byte[0]);
        var cipherOutput = cipher.doFinal(encodedPlaintext.bytes());

        var plaintextValueType = plaintextValue.type().name();
        if (TypeSystem.getDefault().LIST().isTypeOf(plaintextValue)) {
            plaintextValueType = "LIST";
        }

        return new AEADEncryptedProperty(
                profileName,
                cipherOutput,
                iv,
                encodedAad,
                keyId,
                plaintextValueType,
                encodedVersion.majorVersion(),
                encodedVersion.minorVersion());
    }

    Value decrypt(AEADEncryptedProperty encryptedProperty, SecretKey key, Value aad)
            throws InvalidAlgorithmParameterException, NoSuchPaddingException, NoSuchAlgorithmException,
                    InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        var iv = encryptedProperty.iv();
        var aadBytes = aad != null
                ? packValue(aad).bytes()
                : encryptedProperty.encodedAad() != null
                        ? encryptedProperty.encodedAad().bytes()
                        : new byte[0];

        var gcmParameterSpec = new GCMParameterSpec(128, iv);
        // TODO decide if HKDF should be kept
        var encryptionKey = Hkdf.deriveAesKey(key, ENCRYPTION_PURPOSE);
        var cipher = prepareCipher(DECRYPT_MODE, encryptionKey, gcmParameterSpec, aadBytes);

        var plaintext = cipher.doFinal(encryptedProperty.cipherOutput());
        return unpackValue(plaintext);
    }

    private byte[] generateIV() throws NoSuchAlgorithmException {
        var secureRandom = SecureRandom.getInstanceStrong();
        return secureRandom.generateSeed(12);
    }

    private Cipher prepareCipher(int opmode, Key key, AlgorithmParameterSpec params, byte[] aad)
            throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
                    InvalidKeyException {
        var cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(opmode, key, params);
        cipher.updateAAD(aad);
        return cipher;
    }

    private ValueEncoder.Encoded packValue(Value value) {
        var loader = new ValueEncoderFactoryLoader(logging);
        var factory = loader.factory();
        var encoder = factory.create(ValueEncodingSchemeVersion.V1_0);
        try {
            return encoder.encode((InternalValue) value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Value unpackValue(byte[] bytes) {
        var loader = new ValueDecoderFactoryLoader(logging);
        var factory = loader.factory();
        var decoder = factory.create(ValueEncodingSchemeVersion.V1_0, valueFactory);
        try {
            return (Value) decoder.decode(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] packAEAD(AEADEncryptedProperty encryptedProperty) {
        var loader = new PackStreamEncoderFactoryLoader(logging);
        var factory = loader.factory();
        var encoder = factory.create(Set.of(EncryptedStructureEncoder.getInstance()));
        var output = WriteOutputs.bytes();
        try {
            encoder.encode(encryptedProperty.toEncryptedStruct(), output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        var packed = output.output();
        var result = new byte[packed.length + 1];

        result[0] = 1;
        System.arraycopy(packed, 0, result, 1, packed.length);
        return result;
    }

    public AEADEncryptedProperty unpackAEAD(byte[] bytes) {
        var loader = new PackStreamDecoderFactoryLoader(logging);
        var factory = loader.factory();
        var decoder = factory.create(valueFactory, Set.of(EncryptedStructureDecoder.getInstance()));
        var input = ReadInputs.bytes(Arrays.copyOfRange(bytes, 1, bytes.length));
        try {
            var struct = decoder.decodeStructure(input, EncryptedStructure.class);
            return AEADEncryptedProperty.fromEncryptedStruct(struct);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
