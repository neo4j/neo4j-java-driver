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

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.UUID;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.driver.types.Vector;
import org.neo4j.driver.util.Preview;

/**
 * A Neo4j Property encryption request.
 * @since 6.3.0
 * @see PropertyEncryption
 * @see org.neo4j.driver.property_encryption.async.AsyncPropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactive.ReactivePropertyEncryption
 * @see org.neo4j.driver.property_encryption.reactivestreams.ReactivePropertyEncryption
 */
@Preview(name = "Property Encryption")
public interface PropertyEncryptRequest {

    /**
     * A builder step for setting value.
     */
    interface ValueStep {
        /**
         * Sets the value to encrypt.
         * <p>
         * Note that the value MUST be of a supported Neo4j Property Type.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        AADStep fromValue(Value value);

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(boolean value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(LocalDate value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(OffsetTime value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(LocalTime value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(LocalDateTime value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(OffsetDateTime value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(ZonedDateTime value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(Period value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(Duration value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(IsoDuration value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(double value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(int value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(long value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(Point value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(char value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(String value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(Vector value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the value to encrypt.
         *
         * @param value the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(UUID value) {
            return fromValue(Values.value(value));
        }

        /**
         * Sets the values to encrypt.
         *
         * @param values the values to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(byte... values) {
            return fromValue(Values.value(values));
        }

        /**
         * Sets the values to encrypt.
         *
         * @param values the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(String... values) {
            return fromValue(Values.value(values));
        }

        /**
         * Sets the values to encrypt.
         *
         * @param values the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(boolean... values) {
            return fromValue(Values.value(values));
        }

        /**
         * Sets the values to encrypt.
         *
         * @param values the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(char... values) {
            return fromValue(Values.value(values));
        }

        /**
         * Sets the values to encrypt.
         *
         * @param values the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(short... values) {
            return fromValue(Values.value(values));
        }

        /**
         * Sets the values to encrypt.
         *
         * @param values the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(int... values) {
            return fromValue(Values.value(values));
        }

        /**
         * Sets the values to encrypt.
         *
         * @param values the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(long... values) {
            return fromValue(Values.value(values));
        }

        /**
         * Sets the values to encrypt.
         *
         * @param values the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(float... values) {
            return fromValue(Values.value(values));
        }

        /**
         * Sets the values to encrypt.
         *
         * @param values the value to encrypt
         * @return the next builder step
         */
        default AADStep fromValue(double... values) {
            return fromValue(Values.value(values));
        }
    }

    /**
     * A builder step for setting AAD.
     */
    interface AADStep extends ProfileStep {
        /**
         * Adds the supplied value as AAD for encryption request.
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
        ProfileStep withAAD(Value aad);

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value
         * @return the next builder step
         */
        default ProfileStep withAAD(boolean aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default ProfileStep withAAD(LocalDate aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default ProfileStep withAAD(OffsetTime aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default ProfileStep withAAD(LocalTime aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value
         * @return the next builder step
         */
        default ProfileStep withAAD(double aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value
         * @return the next builder step
         */
        default ProfileStep withAAD(int aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value
         * @return the next builder step
         */
        default ProfileStep withAAD(long aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default ProfileStep withAAD(Point aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default ProfileStep withAAD(String aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default ProfileStep withAAD(UUID aad) {
            return withAAD(Values.value(aad));
        }

        /**
         * Adds the supplied value as AAD for encryption request.
         *
         * @param aad the AAD value, {@literal null} disables AAD
         * @return the next builder step
         */
        default ProfileStep withAAD(byte[] aad) {
            return withAAD(Values.value(aad));
        }
    }

    /**
     * A builder step for selection encryption profile.
     */
    interface ProfileStep extends EncryptionKeyReferenceStep {
        /**
         * Sets the profile name to use.
         * @param profileName the profile name
         * @return the next builder step
         */
        EncryptionKeyReferenceStep usingProfile(String profileName);
    }

    /**
     * A builder step for selecting key.
     */
    interface EncryptionKeyReferenceStep extends BuildStep {
        /**
         * Sets the key id to use.
         * @param keyId the key id
         * @return the next builder step
         */
        BuildStep usingKeyId(String keyId);

        /**
         * Sets the key alias to use.
         * @param keyAlias the key alias
         * @return the next builder step
         */
        BuildStep usingKeyAlias(String keyAlias);
    }

    /**
     * A builder step for building {@link PropertyEncryptRequest}.
     */
    interface BuildStep {
        /**
         * Builds and returns a new {@link PropertyEncryptRequest} instance.
         * @return the new request instance
         */
        PropertyEncryptRequest build();
    }
}
