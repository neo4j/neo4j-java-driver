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
package org.neo4j.driver.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.security.SecurityPlans.isCustomized;

import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Config;
import org.neo4j.driver.testutil.TestUtil;

class SecuritySettingsTest {
    @Nested
    class SerializationTests {
        @Test
        void defaultSettingsShouldNotBeCustomizedWhenReadBack() throws IOException, ClassNotFoundException {
            var securitySettings = new SecuritySettings.SecuritySettingsBuilder().build();

            assertFalse(isCustomized(securitySettings));

            var verify = TestUtil.serializeAndReadBack(securitySettings, SecuritySettings.class);

            assertFalse(isCustomized(verify));
        }

        @Test
        void defaultsShouldBeCheckCorrect() throws IOException, ClassNotFoundException {
            var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                    .withoutEncryption()
                    .withTrustStrategy(Config.TrustStrategy.trustSystemCertificates())
                    .build();

            // The settings are still equivalent to the defaults, even if the builder has been used. It is not
            // customized.
            assertFalse(isCustomized(securitySettings));

            var verify = TestUtil.serializeAndReadBack(securitySettings, SecuritySettings.class);

            assertFalse(isCustomized(verify));
        }

        @Test
        void shouldReadBackChangedEncryption() throws IOException, ClassNotFoundException {
            var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                    .withEncryption()
                    .withTrustStrategy(Config.TrustStrategy.trustSystemCertificates())
                    .build();

            assertTrue(isCustomized(securitySettings));
            assertTrue(securitySettings.encrypted());

            var verify = TestUtil.serializeAndReadBack(securitySettings, SecuritySettings.class);

            assertTrue(isCustomized(verify));
            assertTrue(verify.encrypted());
        }

        @Test
        void shouldReadBackChangedStrategey() throws IOException, ClassNotFoundException {
            var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                    .withoutEncryption()
                    .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
                    .build();

            // The settings are still equivalent to the defaults, even if the builder has been used. It is not
            // customized.
            assertTrue(isCustomized(securitySettings));
            assertFalse(securitySettings.encrypted());
            assertEquals(
                    Config.TrustStrategy.trustAllCertificates().strategy(),
                    securitySettings.trustStrategy().strategy());

            var verify = TestUtil.serializeAndReadBack(securitySettings, SecuritySettings.class);

            assertTrue(isCustomized(verify));
            assertFalse(verify.encrypted());
            assertEquals(
                    Config.TrustStrategy.trustAllCertificates().strategy(),
                    verify.trustStrategy().strategy());
        }

        @Test
        void shouldReadBackChangedCertFile() throws IOException, ClassNotFoundException {
            var securitySettings = new SecuritySettings.SecuritySettingsBuilder()
                    .withoutEncryption()
                    .withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(new File("some.cert")))
                    .build();

            // The settings are still equivalent to the defaults, even if the builder has been used. It is not
            // customized.
            assertTrue(isCustomized(securitySettings));
            assertFalse(securitySettings.encrypted());
            assertEquals(
                    Config.TrustStrategy.trustCustomCertificateSignedBy(new File("some.cert"))
                            .strategy(),
                    securitySettings.trustStrategy().strategy());

            var verify = TestUtil.serializeAndReadBack(securitySettings, SecuritySettings.class);

            assertTrue(isCustomized(verify));
            assertFalse(verify.encrypted());
            assertEquals(
                    Config.TrustStrategy.trustCustomCertificateSignedBy(new File("some.cert"))
                            .strategy(),
                    verify.trustStrategy().strategy());
        }
    }
}
