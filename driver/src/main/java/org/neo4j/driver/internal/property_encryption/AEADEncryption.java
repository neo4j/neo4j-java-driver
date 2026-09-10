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
import java.security.Provider;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;
import java.util.Objects;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import org.neo4j.bolt.connection.codec.ReadInputs;
import org.neo4j.bolt.connection.codec.WriteOutputs;
import org.neo4j.bolt.connection.codec.packstream.PackStreamDecoder;
import org.neo4j.bolt.connection.codec.packstream.PackStreamEncoder;
import org.neo4j.bolt.connection.codec.packstream.struct.EncryptedStructure;
import org.neo4j.bolt.connection.codec.value_encoding.ValueDecoder;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncoder;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.types.TypeSystem;

public final class AEADEncryption {
    private static final byte ENCODED_BYTES_VERSION = 1;
    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String ENCRYPTION_PURPOSE = "neo4j/property-encryption/v1";
    private static final String LIST_TYPE = "LIST";
    private static final Value EMPTY_AAD = Values.NULL;
    private final ValueEncoder valueEncoder;
    private final ValueDecoder valueDecoder;
    private final PackStreamEncoder packStreamEncoder;
    private final PackStreamDecoder packStreamDecoder;

    public AEADEncryption(
            ValueEncoder valueEncoder,
            ValueDecoder valueDecoder,
            PackStreamEncoder packStreamEncoder,
            PackStreamDecoder packStreamDecoder) {
        this.valueEncoder = Objects.requireNonNull(valueEncoder);
        this.valueDecoder = Objects.requireNonNull(valueDecoder);
        this.packStreamEncoder = Objects.requireNonNull(packStreamEncoder);
        this.packStreamDecoder = Objects.requireNonNull(packStreamDecoder);
    }

    AEADEncryptedProperty encrypt(
            InternalPropertyEncryptionRequest encryptionRequest,
            SecretKey key,
            String keyId,
            String profileName,
            Provider provider,
            SecureRandom secureRandomIV)
            throws NoSuchAlgorithmException, IllegalBlockSizeException, BadPaddingException,
                    InvalidAlgorithmParameterException, NoSuchPaddingException, InvalidKeyException {
        var plaintextValue = encryptionRequest.value;
        var encodedPlaintext = encode(plaintextValue);
        var encodedVersion = encodedPlaintext.baseVersion();
        var iv = encryptionRequest.iv != null
                ? encryptionRequest.iv // for testing purposes only
                : generateIV(secureRandomIV);
        var gcmParameterSpec = new GCMParameterSpec(128, iv);
        var encodedAad = encryptionRequest.aad != null ? encode(encryptionRequest.aad) : null;
        // TODO decide if HKDF should be kept
        var encryptionKey = Hkdf.deriveAesKey(key, ENCRYPTION_PURPOSE);
        var cipher = prepareCipher(
                Cipher.ENCRYPT_MODE,
                encryptionKey,
                gcmParameterSpec,
                encodedAad != null ? encodedAad.bytes() : new byte[0],
                provider);
        var cipherOutput = cipher.doFinal(encodedPlaintext.bytes());

        var plaintextValueType = plaintextValue.type().name();
        if (TypeSystem.getDefault().LIST().isTypeOf(plaintextValue)) {
            plaintextValueType = LIST_TYPE;
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

    Value decrypt(AEADEncryptedProperty encryptedProperty, SecretKey key, Value aad, Provider provider)
            throws InvalidAlgorithmParameterException, NoSuchPaddingException, NoSuchAlgorithmException,
                    InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        var iv = encryptedProperty.iv();
        var aadBytes = aad != null
                ? encode(aad).bytes()
                : encryptedProperty.encodedAad() != null
                        ? encryptedProperty.encodedAad().bytes()
                        : new byte[0];

        var gcmParameterSpec = new GCMParameterSpec(128, iv);
        // TODO decide if HKDF should be kept
        var encryptionKey = Hkdf.deriveAesKey(key, ENCRYPTION_PURPOSE);
        var cipher = prepareCipher(DECRYPT_MODE, encryptionKey, gcmParameterSpec, aadBytes, provider);

        var plaintext = cipher.doFinal(encryptedProperty.cipherOutput());
        return decode(plaintext);
    }

    private ValueEncoder.Encoded encode(Value value) {
        try {
            return valueEncoder.encode((InternalValue) value);
        } catch (IOException e) {
            throw new ClientException("Failed to encode value", e);
        }
    }

    private Value decode(byte[] bytes) {
        try {
            return (Value) valueDecoder.decode(bytes);
        } catch (IOException e) {
            throw new ClientException("Failed to decode value", e);
        }
    }

    public byte[] encodeToEncryptedBytes(AEADEncryptedProperty encryptedProperty) {
        var output = WriteOutputs.bytes();
        try {
            packStreamEncoder.encode(encryptedProperty.toEncryptedStruct(), output);
        } catch (IOException e) {
            throw new ClientException("Failed to encode Encrypted Structure", e);
        }
        var packed = output.output();
        var result = new byte[packed.length + 1];

        result[0] = ENCODED_BYTES_VERSION;
        System.arraycopy(packed, 0, result, 1, packed.length);
        return result;
    }

    public AEADEncryptedProperty decodeEncryptedBytes(byte[] bytes) {
        Objects.requireNonNull(bytes);
        var version = bytes[0];
        if (version != ENCODED_BYTES_VERSION) {
            throw new BoltClientException("Unsupported encrypted property version: " + version);
        }
        var input = ReadInputs.bytes(Arrays.copyOfRange(bytes, 1, bytes.length));
        EncryptedStructure encryptedStructure;
        try {
            encryptedStructure = packStreamDecoder.decodeStructure(input, EncryptedStructure.class);
        } catch (Exception e) {
            throw new ClientException("Failed to decode Encrypted Structure", e);
        }
        return AEADEncryptedProperty.fromEncryptedStruct(encryptedStructure);
    }

    private byte[] generateIV(SecureRandom secureRandomIV) {
        var iv = new byte[12];
        secureRandomIV.nextBytes(iv);
        return iv;
    }

    private Cipher prepareCipher(int opmode, Key key, AlgorithmParameterSpec params, byte[] aad, Provider provider)
            throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
                    InvalidKeyException {
        var cipher = cipher(provider);
        cipher.init(opmode, key, params);
        cipher.updateAAD(aad);
        return cipher;
    }

    private Cipher cipher(Provider provider) throws NoSuchPaddingException, NoSuchAlgorithmException {
        return provider == null
                ? Cipher.getInstance(CIPHER_TRANSFORMATION)
                : Cipher.getInstance(CIPHER_TRANSFORMATION, provider);
    }
}
