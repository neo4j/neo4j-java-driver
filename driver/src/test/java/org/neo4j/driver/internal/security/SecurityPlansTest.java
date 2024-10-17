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
package org.neo4j.driver.internal.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.RevocationCheckingStrategy.NO_CHECKS;
import static org.neo4j.driver.RevocationCheckingStrategy.STRICT;
import static org.neo4j.driver.RevocationCheckingStrategy.VERIFY_IF_PRESENT;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.Config;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.SecuritySettings;

class SecurityPlansTest {
    private static Stream<String> selfSignedSchemes() {
        return Stream.of("bolt+ssc", "neo4j+ssc");
    }

    private static Stream<String> systemCertSchemes() {
        return Stream.of("neo4j+s", "bolt+s");
    }

    private static Stream<String> unencryptedSchemes() {
        return Stream.of("neo4j", "bolt");
    }

    private static Stream<String> allSecureSchemes() {
        return Stream.concat(selfSignedSchemes(), systemCertSchemes());
    }

    @ParameterizedTest
    @MethodSource("allSecureSchemes")
    void testEncryptionSchemeEnablesEncryption(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder().build();

        var securityPlan = SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none());

        assertTrue(securityPlan.requiresEncryption());
    }

    @ParameterizedTest
    @MethodSource("systemCertSchemes")
    void testSystemCertCompatibleConfiguration(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder().build();

        var securityPlan = SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none());

        assertTrue(securityPlan.requiresEncryption());
        assertTrue(securityPlan.requiresHostnameVerification());
        assertEquals(NO_CHECKS, securityPlan.revocationCheckingStrategy());
    }

    @ParameterizedTest
    @MethodSource("selfSignedSchemes")
    void testSelfSignedCertConfigDisablesHostnameVerification(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder().build();

        var securityPlan = SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none());

        assertTrue(securityPlan.requiresEncryption());
        assertFalse(securityPlan.requiresHostnameVerification());
    }

    @ParameterizedTest
    @MethodSource("allSecureSchemes")
    void testThrowsOnUserCustomizedEncryption(String scheme) {
        var securitySettings =
                new SecuritySettings.SecuritySettingsBuilder().withEncryption().build();

        var ex = assertThrows(
                ClientException.class,
                () -> SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none()));

        assertTrue(ex.getMessage()
                .contains(String.format(
                        "Scheme %s is not configurable with manual encryption and trust settings", scheme)));
    }

    @ParameterizedTest
    @MethodSource("allSecureSchemes")
    void testThrowsOnUserCustomizedTrustConfiguration(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
                .build();

        var ex = assertThrows(
                ClientException.class,
                () -> SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none()));

        assertTrue(ex.getMessage()
                .contains(String.format(
                        "Scheme %s is not configurable with manual encryption and trust settings", scheme)));
    }

    @ParameterizedTest
    @MethodSource("allSecureSchemes")
    void testThrowsOnUserCustomizedTrustConfigurationAndEncryption(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withTrustStrategy(Config.TrustStrategy.trustSystemCertificates())
                .withEncryption()
                .build();

        var ex = assertThrows(
                ClientException.class,
                () -> SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none()));

        assertTrue(ex.getMessage()
                .contains(String.format(
                        "Scheme %s is not configurable with manual encryption and trust settings", scheme)));
    }

    @ParameterizedTest
    @MethodSource("unencryptedSchemes")
    void testNoEncryption(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder().build();

        var securityPlan = SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none());

        assertFalse(securityPlan.requiresEncryption());
    }

    @ParameterizedTest
    @MethodSource("unencryptedSchemes")
    void testConfiguredEncryption(String scheme) {
        var securitySettings =
                new SecuritySettings.SecuritySettingsBuilder().withEncryption().build();

        var securityPlan = SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none());

        assertTrue(securityPlan.requiresEncryption());
    }

    @ParameterizedTest
    @MethodSource("unencryptedSchemes")
    void testConfiguredAllCertificates(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withEncryption()
                .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
                .build();

        var securityPlan = SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none());

        assertTrue(securityPlan.requiresEncryption());
    }

    @ParameterizedTest
    @MethodSource("unencryptedSchemes")
    void testConfigureStrictRevocationChecking(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withTrustStrategy(
                        Config.TrustStrategy.trustSystemCertificates().withStrictRevocationChecks())
                .withEncryption()
                .build();

        var securityPlan = SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none());

        assertEquals(STRICT, securityPlan.revocationCheckingStrategy());
    }

    @ParameterizedTest
    @MethodSource("unencryptedSchemes")
    void testConfigureVerifyIfPresentRevocationChecking(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withTrustStrategy(
                        Config.TrustStrategy.trustSystemCertificates().withVerifyIfPresentRevocationChecks())
                .withEncryption()
                .build();

        var securityPlan = SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none());

        assertEquals(VERIFY_IF_PRESENT, securityPlan.revocationCheckingStrategy());
    }

    @ParameterizedTest
    @MethodSource("unencryptedSchemes")
    void testRevocationCheckingDisabledByDefault(String scheme) {
        var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                .withTrustStrategy(Config.TrustStrategy.trustSystemCertificates())
                .withEncryption()
                .build();

        var securityPlan = SecurityPlans.createSecurityPlan(securitySettings, scheme, null, Logging.none());

        assertEquals(NO_CHECKS, securityPlan.revocationCheckingStrategy());
    }
}
