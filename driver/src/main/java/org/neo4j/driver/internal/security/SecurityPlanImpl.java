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

import static org.neo4j.driver.RevocationCheckingStrategy.VERIFY_IF_PRESENT;
import static org.neo4j.driver.RevocationCheckingStrategy.requiresRevocationChecking;
import static org.neo4j.driver.internal.util.CertificateTool.loadX509Cert;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.neo4j.driver.RevocationCheckingStrategy;

/**
 * A SecurityPlan consists of encryption and trust details.
 */
public class SecurityPlanImpl implements SecurityPlan {
    public static SecurityPlan forAllCertificates(
            boolean requiresHostnameVerification, RevocationCheckingStrategy revocationCheckingStrategy)
            throws GeneralSecurityException {
        var sslContext = SSLContext.getInstance("TLS");
        sslContext.init(new KeyManager[0], new TrustManager[] {new TrustAllTrustManager()}, null);

        return new SecurityPlanImpl(true, sslContext, requiresHostnameVerification, revocationCheckingStrategy);
    }

    public static SecurityPlan forCustomCASignedCertificates(
            List<File> certFiles,
            boolean requiresHostnameVerification,
            RevocationCheckingStrategy revocationCheckingStrategy)
            throws GeneralSecurityException, IOException {
        var sslContext = configureSSLContext(certFiles, revocationCheckingStrategy);
        return new SecurityPlanImpl(true, sslContext, requiresHostnameVerification, revocationCheckingStrategy);
    }

    public static SecurityPlan forSystemCASignedCertificates(
            boolean requiresHostnameVerification, RevocationCheckingStrategy revocationCheckingStrategy)
            throws GeneralSecurityException, IOException {
        var sslContext = configureSSLContext(Collections.emptyList(), revocationCheckingStrategy);
        return new SecurityPlanImpl(true, sslContext, requiresHostnameVerification, revocationCheckingStrategy);
    }

    private static SSLContext configureSSLContext(
            List<File> customCertFiles, RevocationCheckingStrategy revocationCheckingStrategy)
            throws GeneralSecurityException, IOException {
        var trustedKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustedKeyStore.load(null, null);

        if (!customCertFiles.isEmpty()) {
            // Certificate files are specified, so we will load the certificates in the file
            loadX509Cert(customCertFiles, trustedKeyStore);
        } else {
            loadSystemCertificates(trustedKeyStore);
        }

        var pkixBuilderParameters = configurePKIXBuilderParameters(trustedKeyStore, revocationCheckingStrategy);

        var sslContext = SSLContext.getInstance("TLS");
        var trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        if (pkixBuilderParameters == null) {
            trustManagerFactory.init(trustedKeyStore);
        } else {
            trustManagerFactory.init(new CertPathTrustManagerParameters(pkixBuilderParameters));
        }

        sslContext.init(new KeyManager[0], trustManagerFactory.getTrustManagers(), null);

        return sslContext;
    }

    private static PKIXBuilderParameters configurePKIXBuilderParameters(
            KeyStore trustedKeyStore, RevocationCheckingStrategy revocationCheckingStrategy)
            throws InvalidAlgorithmParameterException, KeyStoreException {
        PKIXBuilderParameters pkixBuilderParameters = null;

        if (requiresRevocationChecking(revocationCheckingStrategy)) {
            // Configure certificate revocation checking (X509CertSelector() selects all certificates)
            pkixBuilderParameters = new PKIXBuilderParameters(trustedKeyStore, new X509CertSelector());

            // sets checking of stapled ocsp response
            pkixBuilderParameters.setRevocationEnabled(true);

            // enables status_request extension in client hello
            System.setProperty("jdk.tls.client.enableStatusRequestExtension", "true");

            if (revocationCheckingStrategy.equals(VERIFY_IF_PRESENT)) {
                // enables soft-fail behaviour if no stapled response found.
                Security.setProperty("ocsp.enable", "true");
            }
        }
        return pkixBuilderParameters;
    }

    private static void loadSystemCertificates(KeyStore trustedKeyStore) throws GeneralSecurityException {
        // To customize the PKIXParameters we need to get hold of the default KeyStore, no other elegant way available
        var tempFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tempFactory.init((KeyStore) null);

        // Get hold of the default trust manager
        var x509TrustManager = (X509TrustManager) Arrays.stream(tempFactory.getTrustManagers())
                .filter(trustManager -> trustManager instanceof X509TrustManager)
                .findFirst()
                .orElse(null);

        if (x509TrustManager == null) {
            throw new CertificateException("No system certificates found");
        } else {
            // load system default certificates into KeyStore
            loadX509Cert(x509TrustManager.getAcceptedIssuers(), trustedKeyStore);
        }
    }

    public static SecurityPlan insecure() {
        return new SecurityPlanImpl(false, null, false, RevocationCheckingStrategy.NO_CHECKS);
    }

    private final boolean requiresEncryption;
    private final SSLContext sslContext;
    private final boolean requiresHostnameVerification;
    private final RevocationCheckingStrategy revocationCheckingStrategy;

    private SecurityPlanImpl(
            boolean requiresEncryption,
            SSLContext sslContext,
            boolean requiresHostnameVerification,
            RevocationCheckingStrategy revocationCheckingStrategy) {
        this.requiresEncryption = requiresEncryption;
        this.sslContext = sslContext;
        this.requiresHostnameVerification = requiresHostnameVerification;
        this.revocationCheckingStrategy = revocationCheckingStrategy;
    }

    @Override
    public boolean requiresEncryption() {
        return requiresEncryption;
    }

    @Override
    public SSLContext sslContext() {
        return sslContext;
    }

    @Override
    public boolean requiresHostnameVerification() {
        return requiresHostnameVerification;
    }

    @Override
    public RevocationCheckingStrategy revocationCheckingStrategy() {
        return revocationCheckingStrategy;
    }

    private static class TrustAllTrustManager implements X509TrustManager {
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            throw new CertificateException("All client connections to this client are forbidden.");
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // all fine, pass through
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
