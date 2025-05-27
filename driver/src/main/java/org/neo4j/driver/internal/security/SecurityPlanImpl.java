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

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.neo4j.bolt.connection.ssl.SSLContexts;
import org.neo4j.bolt.connection.ssl.TrustManagerFactories;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.Logging;
import org.neo4j.driver.RevocationCheckingStrategy;
import org.neo4j.driver.internal.util.Futures;

/**
 * A SecurityPlan consists of encryption and trust details.
 */
public class SecurityPlanImpl implements SecurityPlan {

    public static SecurityPlan forAllCertificates(
            boolean requiresHostnameVerification,
            RevocationCheckingStrategy revocationCheckingStrategy,
            ClientCertificateManager clientCertificateManager,
            @SuppressWarnings("deprecation") Logging logging)
            throws NoSuchAlgorithmException, KeyManagementException {
        return new SecurityPlanImpl(
                SSLContexts::forAnyCertificate,
                requiresHostnameVerification,
                revocationCheckingStrategy,
                clientCertificateManager,
                logging);
    }

    public static SecurityPlan forCustomCASignedCertificates(
            List<File> certFiles,
            boolean requiresHostnameVerification,
            RevocationCheckingStrategy revocationCheckingStrategy,
            ClientCertificateManager clientCertificateManager,
            @SuppressWarnings("deprecation") Logging logging)
            throws GeneralSecurityException, IOException {
        var trustManagerFactory = TrustManagerFactories.forCertificates(certFiles, map(revocationCheckingStrategy));
        return new SecurityPlanImpl(
                keyManagers -> SSLContexts.forTrustManagers(keyManagers, trustManagerFactory.getTrustManagers()),
                requiresHostnameVerification,
                revocationCheckingStrategy,
                clientCertificateManager,
                logging);
    }

    public static SecurityPlan forSystemCASignedCertificates(
            boolean requiresHostnameVerification,
            RevocationCheckingStrategy revocationCheckingStrategy,
            ClientCertificateManager clientCertificateManager,
            @SuppressWarnings("deprecation") Logging logging)
            throws GeneralSecurityException, IOException {
        var trustManagerFactory = TrustManagerFactories.forSystemCertificates(map(revocationCheckingStrategy));
        return new SecurityPlanImpl(
                keyManagers -> SSLContexts.forTrustManagers(keyManagers, trustManagerFactory.getTrustManagers()),
                requiresHostnameVerification,
                revocationCheckingStrategy,
                clientCertificateManager,
                logging);
    }

    public static SecurityPlan insecure() {
        return new SecurityPlanImpl();
    }

    private final boolean requiresEncryption;
    private final boolean requiresClientAuth;
    private final boolean requiresHostnameVerification;
    private final RevocationCheckingStrategy revocationCheckingStrategy;
    private final Supplier<CompletionStage<SSLContext>> sslContextSupplier;

    private SecurityPlanImpl(
            SSLContextSupplier sslContextSupplier,
            boolean requiresHostnameVerification,
            RevocationCheckingStrategy revocationCheckingStrategy,
            ClientCertificateManager clientCertificateManager,
            @SuppressWarnings("deprecation") Logging logging)
            throws NoSuchAlgorithmException, KeyManagementException {
        this.requiresEncryption = true;
        this.requiresHostnameVerification = requiresHostnameVerification;
        this.revocationCheckingStrategy = revocationCheckingStrategy;
        var sslContextManager = new SSLContextManager(clientCertificateManager, sslContextSupplier, logging);
        this.sslContextSupplier = sslContextManager::getSSLContext;
        this.requiresClientAuth = clientCertificateManager != null;
    }

    private SecurityPlanImpl() {
        this.requiresEncryption = false;
        this.requiresHostnameVerification = false;
        this.revocationCheckingStrategy = RevocationCheckingStrategy.NO_CHECKS;
        this.sslContextSupplier = Futures::completedWithNull;
        this.requiresClientAuth = false;
    }

    @Override
    public boolean requiresEncryption() {
        return requiresEncryption;
    }

    @Override
    public boolean requiresClientAuth() {
        return requiresClientAuth;
    }

    @Override
    public CompletionStage<SSLContext> sslContext() {
        return sslContextSupplier.get();
    }

    @Override
    public boolean requiresHostnameVerification() {
        return requiresHostnameVerification;
    }

    @Override
    public RevocationCheckingStrategy revocationCheckingStrategy() {
        return revocationCheckingStrategy;
    }

    private static org.neo4j.bolt.connection.ssl.RevocationCheckingStrategy map(
            RevocationCheckingStrategy revocationCheckingStrategy) {
        return switch (revocationCheckingStrategy) {
            case NO_CHECKS -> org.neo4j.bolt.connection.ssl.RevocationCheckingStrategy.NO_CHECKS;
            case VERIFY_IF_PRESENT -> org.neo4j.bolt.connection.ssl.RevocationCheckingStrategy.VERIFY_IF_PRESENT;
            case STRICT -> org.neo4j.bolt.connection.ssl.RevocationCheckingStrategy.STRICT;
        };
    }
}
