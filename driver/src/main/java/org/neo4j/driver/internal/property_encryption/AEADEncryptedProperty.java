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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeMap;
import org.neo4j.bolt.connection.codec.packstream.struct.EncryptedStructure;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncoder;
import org.neo4j.bolt.connection.codec.value_encoding.ValueEncodingSchemeVersion;
import org.neo4j.bolt.connection.values.Value;
import org.neo4j.driver.Values;

public record AEADEncryptedProperty(
        String profileName,
        byte[] cipherOutput,
        byte[] iv,
        ValueEncoder.Encoded encodedAad,
        String keyId,
        String typeName,
        long typeEncodingSchemeMajor,
        long typeEncodingSchemeMinor) {
    private static final Comparator<String> UTF8_COMPARATOR =
            (a, b) -> Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    private static final String IV = "iv";
    private static final String AAD = "aad";
    private static final String AAD_ENCODING_SCHEME_MAJOR = "aad_encoding_scheme_major";
    private static final String AAD_ENCODING_SCHEME_MINOR = "aad_encoding_scheme_minor";
    private static final String KEY_ID = "key_id";

    EncryptedStructure toEncryptedStruct() {
        var metadata = new TreeMap<String, Value>(UTF8_COMPARATOR);
        metadata.put(IV, (Value) Values.value(iv));
        if (encodedAad != null) {
            metadata.put(AAD, (Value) Values.value(encodedAad.bytes()));
            metadata.put(AAD_ENCODING_SCHEME_MAJOR, (Value)
                    Values.value(encodedAad.baseVersion().majorVersion()));
            metadata.put(AAD_ENCODING_SCHEME_MINOR, (Value)
                    Values.value(encodedAad.baseVersion().minorVersion()));
        }
        metadata.put(KEY_ID, (Value) Values.value(keyId));
        return new EncryptedStructure(
                profileName, cipherOutput, typeName, typeEncodingSchemeMajor, typeEncodingSchemeMinor, metadata);
    }

    static AEADEncryptedProperty fromEncryptedStruct(EncryptedStructure encryptedStruct) {
        var metadata = encryptedStruct.metadata();
        var iv = metadata.get(IV).asByteArray();
        var aad = metadata.get(AAD);
        ValueEncoder.Encoded encodedAad = null;
        if (aad != null) {
            var aadBytes = aad.asByteArray();
            var aadSerializationSchemeMajor =
                    metadata.get(AAD_ENCODING_SCHEME_MAJOR).asLong();
            var aadSerializationSchemeMinor =
                    metadata.get(AAD_ENCODING_SCHEME_MINOR).asLong();
            encodedAad = new ValueEncoder.Encoded(
                    aadBytes, new ValueEncodingSchemeVersion((int) aadSerializationSchemeMajor, (int)
                            aadSerializationSchemeMinor));
        }
        var keyId = metadata.get(KEY_ID).asString();
        return new AEADEncryptedProperty(
                encryptedStruct.profileName(),
                encryptedStruct.cipherOutput(),
                iv,
                encodedAad,
                keyId,
                encryptedStruct.typeName(),
                encryptedStruct.typeEncodingSchemeMajor(),
                encryptedStruct.typeEncodingSchemeMinor());
    }
}
