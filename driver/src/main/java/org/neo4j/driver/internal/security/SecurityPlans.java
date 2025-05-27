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

import static org.neo4j.driver.internal.Scheme.isHighTrustScheme;
import static org.neo4j.driver.internal.Scheme.isSecurityScheme;
import static org.neo4j.driver.internal.security.SecurityPlanImpl.insecure;

import java.io.IOException;
import java.security.GeneralSecurityException;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.Logging;
import org.neo4j.driver.RevocationCheckingStrategy;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.Scheme;
import org.neo4j.driver.internal.SecuritySettings;

public class SecurityPlans {
    public static SecurityPlan createSecurityPlan(
            SecuritySettings settings,
            String uriScheme,
            ClientCertificateManager clientCertificateManager,
            @SuppressWarnings("deprecation") Logging logging) {
        Scheme.validateScheme(uriScheme);
        try {
            if (isSecurityScheme(uriScheme)) {
                assertSecuritySettingsNotUserConfigured(settings, uriScheme);
                return createSecurityPlanFromScheme(uriScheme, clientCertificateManager, logging);
            } else {
                return createSecurityPlanImpl(
                        settings.encrypted(), settings.trustStrategy(), clientCertificateManager, logging);
            }
        } catch (GeneralSecurityException | IOException ex) {
            var message = "Unable to establish SSL parameters";
            throw new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    ex);
        }
    }

    private static void assertSecuritySettingsNotUserConfigured(SecuritySettings settings, String uriScheme) {
        if (isCustomized(settings)) {
            var message =
                    String.format("Scheme %s is not configurable with manual encryption and trust settings", uriScheme);
            throw new ClientException(
                    GqlStatusError.UNKNOWN.getStatus(),
                    GqlStatusError.UNKNOWN.getStatusDescription(message),
                    "N/A",
                    message,
                    GqlStatusError.DIAGNOSTIC_RECORD,
                    null);
        }
    }

    public static boolean isCustomized(SecuritySettings securitySettings) {
        return !(SecuritySettings.DEFAULT.encrypted() == securitySettings.encrypted()
                && hasEqualTrustStrategy(securitySettings));
    }

    private static boolean hasEqualTrustStrategy(SecuritySettings settings) {
        var t1 = SecuritySettings.DEFAULT.trustStrategy();
        var t2 = settings.trustStrategy();
        if (t1 == t2) {
            return true;
        }

        return t1.isHostnameVerificationEnabled() == t2.isHostnameVerificationEnabled()
                && t1.strategy() == t2.strategy()
                && t1.certFiles().equals(t2.certFiles())
                && t1.revocationCheckingStrategy() == t2.revocationCheckingStrategy();
    }

    private static SecurityPlan createSecurityPlanFromScheme(
            String scheme,
            ClientCertificateManager clientCertificateManager,
            @SuppressWarnings("deprecation") Logging logging)
            throws GeneralSecurityException, IOException {
        if (isHighTrustScheme(scheme)) {
            return SecurityPlanImpl.forSystemCASignedCertificates(
                    true, RevocationCheckingStrategy.NO_CHECKS, clientCertificateManager, logging);
        } else {
            return SecurityPlanImpl.forAllCertificates(
                    false, RevocationCheckingStrategy.NO_CHECKS, clientCertificateManager, logging);
        }
    }

    /*
     * Establish a complete SecurityPlan based on the details provided for
     * driver construction.
     */
    private static SecurityPlan createSecurityPlanImpl(
            boolean encrypted,
            Config.TrustStrategy trustStrategy,
            ClientCertificateManager clientCertificateManager,
            @SuppressWarnings("deprecation") Logging logging)
            throws GeneralSecurityException, IOException {
        if (encrypted) {
            var hostnameVerificationEnabled = trustStrategy.isHostnameVerificationEnabled();
            var revocationCheckingStrategy = trustStrategy.revocationCheckingStrategy();
            return switch (trustStrategy.strategy()) {
                case TRUST_CUSTOM_CA_SIGNED_CERTIFICATES -> SecurityPlanImpl.forCustomCASignedCertificates(
                        trustStrategy.certFiles(),
                        hostnameVerificationEnabled,
                        revocationCheckingStrategy,
                        clientCertificateManager,
                        logging);
                case TRUST_SYSTEM_CA_SIGNED_CERTIFICATES -> SecurityPlanImpl.forSystemCASignedCertificates(
                        hostnameVerificationEnabled, revocationCheckingStrategy, clientCertificateManager, logging);
                case TRUST_ALL_CERTIFICATES -> SecurityPlanImpl.forAllCertificates(
                        hostnameVerificationEnabled, revocationCheckingStrategy, clientCertificateManager, logging);
            };
        } else {
            return insecure();
        }
    }
}
