/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import static org.neo4j.driver.internal.Scheme.isHighTrustScheme;
import static org.neo4j.driver.internal.Scheme.isSecurityScheme;
import static org.neo4j.driver.internal.security.SecurityPlanImpl.insecure;

import java.io.IOException;
import java.security.GeneralSecurityException;
import org.neo4j.driver.Config;
import org.neo4j.driver.RevocationCheckingStrategy;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.Scheme;
import org.neo4j.driver.internal.SecuritySettings;

public class SecurityPlans {
    public static SecurityPlan createSecurityPlan(SecuritySettings settings, String uriScheme) {
        Scheme.validateScheme(uriScheme);
        try {
            if (isSecurityScheme(uriScheme)) {
                assertSecuritySettingsNotUserConfigured(settings, uriScheme);
                return createSecurityPlanFromScheme(uriScheme);
            } else {
                return createSecurityPlanImpl(settings.encrypted(), settings.trustStrategy());
            }
        } catch (GeneralSecurityException | IOException ex) {
            throw new ClientException("Unable to establish SSL parameters", ex);
        }
    }

    private static void assertSecuritySettingsNotUserConfigured(SecuritySettings settings, String uriScheme) {
        if (isCustomized(settings)) {
            throw new ClientException(String.format(
                    "Scheme %s is not configurable with manual encryption and trust settings", uriScheme));
        }
    }

    public static boolean isCustomized(SecuritySettings securitySettings) {
        return !(SecuritySettings.DEFAULT.encrypted() == securitySettings.encrypted()
                && hasEqualTrustStrategy(securitySettings));
    }

    private static boolean hasEqualTrustStrategy(SecuritySettings settings) {
        Config.TrustStrategy t1 = SecuritySettings.DEFAULT.trustStrategy();
        Config.TrustStrategy t2 = settings.trustStrategy();
        if (t1 == t2) {
            return true;
        }

        return t1.isHostnameVerificationEnabled() == t2.isHostnameVerificationEnabled()
                && t1.strategy() == t2.strategy()
                && t1.certFiles().equals(t2.certFiles())
                && t1.revocationCheckingStrategy() == t2.revocationCheckingStrategy();
    }

    private static SecurityPlan createSecurityPlanFromScheme(String scheme)
            throws GeneralSecurityException, IOException {
        if (isHighTrustScheme(scheme)) {
            return SecurityPlanImpl.forSystemCASignedCertificates(true, RevocationCheckingStrategy.NO_CHECKS);
        } else {
            return SecurityPlanImpl.forAllCertificates(false, RevocationCheckingStrategy.NO_CHECKS);
        }
    }

    /*
     * Establish a complete SecurityPlan based on the details provided for
     * driver construction.
     */
    private static SecurityPlan createSecurityPlanImpl(boolean encrypted, Config.TrustStrategy trustStrategy)
            throws GeneralSecurityException, IOException {
        if (encrypted) {
            boolean hostnameVerificationEnabled = trustStrategy.isHostnameVerificationEnabled();
            RevocationCheckingStrategy revocationCheckingStrategy = trustStrategy.revocationCheckingStrategy();
            switch (trustStrategy.strategy()) {
                case TRUST_CUSTOM_CA_SIGNED_CERTIFICATES:
                    return SecurityPlanImpl.forCustomCASignedCertificates(
                            trustStrategy.certFiles(), hostnameVerificationEnabled, revocationCheckingStrategy);
                case TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
                    return SecurityPlanImpl.forSystemCASignedCertificates(
                            hostnameVerificationEnabled, revocationCheckingStrategy);
                case TRUST_ALL_CERTIFICATES:
                    return SecurityPlanImpl.forAllCertificates(hostnameVerificationEnabled, revocationCheckingStrategy);
                default:
                    throw new ClientException("Unknown TLS authentication strategy: "
                            + trustStrategy.strategy().name());
            }
        } else {
            return insecure();
        }
    }
}
