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

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Bare minimum parser for DER-encoded values. The support is sufficient
 * to parse private and public keys.
 * <p>
 * DER(Distinguished Encoding Rules) is a subset of BER(Basic Encoding Rules)
 * with some limitations; length needs to be in a definite form and string/array
 * values must use primitive encoding.
 * <p>
 * Type structure is very simple: {@code IDENTIFIER LENGTH DATA}
 */
final class DerUtils {
    private static final byte INTEGER_TAG = 0x02;
    private static final byte OCTET_STRING_TAG = 0x04;
    private static final byte SEQUENCE_TAG = 0x30;

    private DerUtils() {}

    /**
     * Read an integer.
     *
     * @param input buffer positioned at the beginning of an integer.
     * @return the integer at the current position.
     */
    static BigInteger readDerInteger(ByteBuffer input) {
        var len = der(input, INTEGER_TAG);
        var value = new byte[len];
        input.get(value);
        return new BigInteger(1, value);
    }

    /**
     * Read an octet string, more commonly known as a byte array.
     *
     * @param input buffer positioned at the beginning if an octet string.
     * @return the octet string at the current position.
     */
    static byte[] readDerOctetString(ByteBuffer input) {
        var len = der(input, OCTET_STRING_TAG);
        var value = new byte[len];
        input.get(value);
        return value;
    }

    /**
     * Return the context specific data with tag {@code contextTag}.
     * <p>
     * To break ambiguity, optional values with the same type can be tagged with numbers.
     * In this case, the tag value represents the tag number, and the tag type is inferred.
     *
     * @param input buffer positioned at the beginning if the context specific data.
     * @param contextTag the tag to which data we should find.
     * @return the context data that is tagged with {@code contextTag}, or {@code null} if
     * the tag is not present.
     */
    @SuppressWarnings("SameParameterValue")
    static byte[] getDerContext(ByteBuffer input, byte contextTag) {
        var begin = input.position();
        while (input.hasRemaining()) {
            var start = input.position();
            var tag = input.get();
            var length = getLength(input);
            if (isContextSpecific(tag, contextTag)) {
                // Found our tag, copy full block
                return copyContext(input, start);
            }
            // Skip this block
            input.position(input.position() + length);
        }
        input.position(begin); // Reset input
        return null; // Found no matching tag
    }

    /**
     * Begin parsing a sequence.
     *
     * @param input buffer positioned at the beginning of a sequence.
     * @return length of sequence.
     */
    static int beginDerSequence(ByteBuffer input) {
        return der(input, SEQUENCE_TAG);
    }

    /**
     * Consumes a tag.
     *
     * @param input buffer positions at the beginning of a tag.
     * @param expectedTag what tag should be found.
     * @return the length of the data following the tag.
     * @throws IllegalArgumentException if the correct that is not found.
     */
    private static int der(ByteBuffer input, int expectedTag) {
        var tag = unsignedByte(input);
        if (tag != expectedTag) {
            throw new IllegalArgumentException(format("Expected tag '%02X' but found '%02X'", expectedTag, tag));
        }
        return getLength(input);
    }

    private static byte[] copyContext(ByteBuffer input, int dataStart) {
        input.position(dataStart);
        var tag = input.get();

        if (isConstructed(tag)) {
            var length = getLength(input);
            var data = new byte[length];
            input.get(data);

            // Technically, we should read all data and concatenate "sub-values". However,
            // this is not allowed in DER, so the only valid data consists of 0 or 1
            // "sub-values", which our code works for.
            return data;
        }
        throw new IllegalArgumentException("Unable to extract non-constructed data.");
    }

    /**
     * Determines whether a tag matches the context tag. Context is defined as
     * {@code [10tttttt]}, where 't' is the context tag.
     *
     * @param tag read tag from encoding.
     * @param contextTag the context value we are looking for.
     * @return {@code true} if {@code tag} matches {@code contextTag}, {@code false} otherwise.
     */
    private static boolean isContextSpecific(byte tag, byte contextTag) {
        if ((tag & 0b1100_0000) == 0b1000_0000) {
            return (tag & 0b001_1111) == contextTag;
        }
        return false;
    }

    /**
     * Determines whether a tag is constructed, which is indicated by bit 6 being one.
     * A constructed value is an encapsulation structure, that contains 0 or more
     * "sub-values" that should be concatenated to form the final value.
     *
     * @return {@code true} if the value is constructed.
     */
    private static boolean isConstructed(byte tag) {
        return ((tag & 0b0010_0000) == 0b0010_0000);
    }

    /**
     * Parse the length octet's. They follow the simple scheme of:
     * <pre>
     *   Short form: [0xxxxxxx]
     *   Long form: [1nnnnnnn]...[n-1][n] // where 'n' denotes the number of bytes
     *     n = 0 // indefinite length, not allowed in DER
     *     n = 127 // reserved
     * </pre>
     *
     * @return the length of the following content.
     */
    private static int getLength(ByteBuffer input) {
        var lengthByte = unsignedByte(input);
        // if bit 8 is 0, short form
        if ((lengthByte & 0b1000_0000) == 0) {
            return lengthByte;
        }
        // otherwise, long form
        lengthByte &= 0b0111_1111;
        if (lengthByte == 0) {
            throw new UnsupportedOperationException("Indefinite length is not allowed in DER.");
        }
        if (lengthByte > 2) { // 65mb should be enough, right?
            throw new IllegalArgumentException("Too big content.");
        }
        var len = 0;
        while (lengthByte-- > 0) {
            len <<= Byte.SIZE;
            len |= unsignedByte(input);
        }
        return len;
    }

    /**
     * Because Java...
     */
    private static int unsignedByte(ByteBuffer input) {
        return input.get() & 0xFF;
    }
}
