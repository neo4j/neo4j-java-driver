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
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

final class Hkdf {
    private static final String HMAC_ALG = "HmacSHA256";
    private static final int HASH_LEN = 32;

    private Hkdf() {}

    public static byte[] derive(byte[] ikm, byte[] salt, byte[] info, int length)
            throws NoSuchAlgorithmException, InvalidKeyException {
        // ---- Extract ----

        if (salt == null || salt.length == 0) {
            salt = new byte[HASH_LEN];
        }

        var mac = Mac.getInstance(HMAC_ALG);
        mac.init(new SecretKeySpec(salt, HMAC_ALG));

        var prk = mac.doFinal(ikm);

        // ---- Expand ----

        var okm = new byte[length];

        var previous = new byte[0];
        var offset = 0;
        byte counter = 1;

        while (offset < length) {

            mac.init(new SecretKeySpec(prk, HMAC_ALG));

            mac.update(previous);
            mac.update(info);
            mac.update(counter);

            previous = mac.doFinal();

            var remaining = length - offset;
            var chunk = Math.min(remaining, previous.length);

            System.arraycopy(previous, 0, okm, offset, chunk);

            offset += chunk;
            counter++;
        }

        return okm;
    }

    public static SecretKey deriveAesKey(SecretKey masterKey, String purpose)
            throws NoSuchAlgorithmException, InvalidKeyException {

        var derived = derive(masterKey.getEncoded(), null, purpose.getBytes(StandardCharsets.UTF_8), 32);

        return new SecretKeySpec(derived, "AES");
    }
}
