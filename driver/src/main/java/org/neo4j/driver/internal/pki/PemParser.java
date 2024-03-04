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
package org.neo4j.driver.internal.pki;

import static java.lang.String.format;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.KeyException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Parse a public or private key.
 * <p>
 * In contrast to {@link CertificateFactory}, Java's {@link KeyFactory}'s does not support reading textual pem files.
 *  PEM format reference: <a href="https://datatracker.ietf.org/doc/html/rfc7468">RFC7468</a>
 * This implementation has support for
 * <li>PKCS#8/X.509 Public Key
 * <li>PKCS#8 and encrypted PKCS#8 private keys
 * <li>PKCS#1 encoded RSA, DSA and EC.
 * <li>Encrypted PKCS#1 encoded RSA, DSA and EC.
 */
public final class PemParser {
    private static final String BEGIN = "-----BEGIN ";
    private static final String END = "-----END ";

    /**
     * Mime decoder is almost identical to PEM specification.
     * <p>
     * <a href="https://datatracker.ietf.org/doc/html/rfc2045#section-6.8">RFC2045</a>
     */
    private static final Base64.Decoder BASE_64 = Base64.getMimeDecoder();

    private static final Map<String, PemFormats.PemFormat> parsers;

    static {
        parsers = Map.of(
                PemFormats.Pkcs8.PUBLIC_LABEL, new PemFormats.Pkcs8(),
                PemFormats.Pkcs8.PRIVATE_LABEL, new PemFormats.Pkcs8(),
                PemFormats.Pkcs8Encrypted.ENCRYPTED_LABEL, new PemFormats.Pkcs8Encrypted(),
                PemFormats.PemPKCS1Rsa.PUBLIC_LABEL, new PemFormats.PemPKCS1Rsa(),
                PemFormats.PemPKCS1Rsa.PRIVATE_LABEL, new PemFormats.PemPKCS1Rsa(),
                PemFormats.PemPKCS1Dsa.PRIVATE_LABEL, new PemFormats.PemPKCS1Dsa(),
                PemFormats.PemPKCS1Ec.PRIVATE_LABEL, new PemFormats.PemPKCS1Ec());
    }

    private String label;
    private byte[] der;
    private final Map<String, String> headers = new HashMap<>();

    public PemParser(InputStream in) throws IOException {
        try (var reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII))) {
            parse(reader);
        }
    }

    public PrivateKey getPrivateKey(String password) throws KeyException {
        var pemFormat = parsers.get(label);
        if (pemFormat != null) {
            return pemFormat.decodePrivate(der, headers, password);
        }

        throw new KeyException(format("Provided PEM does not contain a private key, found '%s'.", label));
    }

    public PublicKey getPublicKey() throws KeyException {
        var pemFormat = parsers.get(label);
        if (pemFormat != null) {
            return pemFormat.decodePublicKey(der);
        }
        throw new KeyException(format("Provided PEM does not contain a public key, found '%s'.", label));
    }

    /**
     * Read the input stream and extract all the components.
     * @throws IOException on failure to read from the provided input stream.
     */
    private void parse(BufferedReader reader) throws IOException {
        // Find header, explanatory text is allowed before and after block, so ignore that
        var line = reader.readLine();
        while (line != null && !line.startsWith(BEGIN)) {
            line = reader.readLine();
        }
        if (line == null) {
            throw new IllegalStateException("File does not contain " + BEGIN + " encapsulation boundary.");
        }

        // Get label
        label = extractLabel(line);
        var endMarker = END + label;

        // Extract base64 encoded DER
        var sb = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            if (line.contains(endMarker)) {
                der = BASE_64.decode(sb.toString());
                return;
            }

            // Look for headers, is allowed in legacy PEM
            if (line.contains(":")) {
                var kv = line.split(":");
                headers.put(kv[0].trim(), kv[1].trim());
                continue;
            }

            sb.append(line.trim());
        }
        throw new IllegalStateException("Missing footer: " + endMarker + ".");
    }

    /**
     * Extract the label from the format {@code -----BEGIN (label)-----}.
     * @param line line containing the pre-encapsulation boundary.
     * @return the label for the encapsulation.
     */
    private static String extractLabel(String line) {
        line = line.substring(BEGIN.length());
        var index = line.indexOf('-');

        if (!line.endsWith("-----") || (line.length() - index) != "-----".length()) {
            throw new IllegalStateException(
                    format("Unable to find label, expecting '-----BEGIN (label)-----' but found '%s'.", line));
        }
        return line.substring(0, index);
    }
}
