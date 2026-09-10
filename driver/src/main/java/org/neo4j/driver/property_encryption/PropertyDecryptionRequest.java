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
package org.neo4j.driver.property_encryption;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.util.UUID;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.property_encryption.InternalPropertyDecryptionRequest;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.driver.util.Preview;

/**
 * A Neo4j Property decryption request.
 * @since 6.3.0
 * @see PropertyEncryption
 * @see org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption
 */
@Preview(name = "Property Encryption")
public interface PropertyDecryptionRequest {
    /**
     * Returns a new instance of a build stage for {@link PropertyDecryptionRequest}.
     * @return a new instance of a build stage
     */
    static PropertyDecryptionRequest.ValueStep builder() {
        return new InternalPropertyDecryptionRequest();
    }

    /**
     * A builder step for setting value.
     */
    interface ValueStep {
        /**
         * Sets the value to decrypt.
         * @param value the value to decrypt
         * @return the next builder step
         */
        AADStep fromValue(byte[] value);
    }

    /**
     * A builder step for setting AAD.
     */
    interface AADStep {
        /**
         * Adds the supplied value as AAD for decryption request.
         * <p>
         * Note that only a subset of types is supported for AAD, they are listed below:
         * <ul>
         *     <li>{@link TypeSystem#BOOLEAN()}</li>
         *     <li>{@link TypeSystem#BYTES()}</li>
         *     <li>{@link TypeSystem#STRING()}</li>
         *     <li>{@link TypeSystem#INTEGER()}</li>
         *     <li>{@link TypeSystem#POINT()}</li>
         *     <li>{@link TypeSystem#DATE()}</li>
         *     <li>{@link TypeSystem#TIME()}</li>
         *     <li>{@link TypeSystem#LOCAL_TIME()}</li>
         *     <li>{@link TypeSystem#UUID()}</li>
         * </ul>
         *
         * @param aad the AAD value, both {@literal null} and {@link TypeSystem#NULL()} disable AAD
         * @return the next builder step
         */
        BuildStep withAAD(Value aad);

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value
         * @return the next builder step
         */
        default BuildStep withAAD(boolean aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default BuildStep withAAD(LocalDate aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default BuildStep withAAD(OffsetTime aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default BuildStep withAAD(LocalTime aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value
         * @return the next builder step
         */
        default BuildStep withAAD(double aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value
         * @return the next builder step
         */
        default BuildStep withAAD(int aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value
         * @return the next builder step
         */
        default BuildStep withAAD(long aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default BuildStep withAAD(Point aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default BuildStep withAAD(String aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default BuildStep withAAD(UUID aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for decryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default BuildStep withAAD(byte[] aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Enables using the persisted AAD.
         * @return the next builder step
         */
        BuildStep withPersistedAAD();
    }

    /**
     * A builder step for building {@link PropertyDecryptionRequest}.
     */
    interface BuildStep {
        /**
         * Builds and returns a new {@link PropertyDecryptionRequest} instance.
         * @return the new request instance
         */
        PropertyDecryptionRequest build();
    }
}
