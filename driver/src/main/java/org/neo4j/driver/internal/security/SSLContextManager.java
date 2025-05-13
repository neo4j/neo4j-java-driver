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

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.InternalClientCertificate;
import org.neo4j.driver.internal.pki.PemParser;
import org.neo4j.driver.internal.util.Futures;

class SSLContextManager {
    private final ClientCertificateManager clientCertificateManager;
    private final SecurityPlan.SSLContextSupplier sslContextSupplier;

    @SuppressWarnings("deprecation")
    private final Logger logger;

    private CompletableFuture<SSLContext> sslContextFuture;
    private SSLContext sslContext;
    private Throwable throwable;

    public SSLContextManager(
            ClientCertificateManager clientCertificateManager,
            SecurityPlan.SSLContextSupplier sslContextSupplier,
            @SuppressWarnings("deprecation") Logging logging)
            throws NoSuchAlgorithmException, KeyManagementException {
        this.clientCertificateManager = clientCertificateManager;
        this.sslContextSupplier = sslContextSupplier;
        logger = logging.getLog(getClass());

        if (clientCertificateManager == null) {
            var sslContext = sslContextSupplier.get(new KeyManager[0]);
            sslContextFuture = CompletableFuture.completedFuture(sslContext);
        }
    }

    public CompletionStage<SSLContext> getSSLContext() {
        return clientCertificateManager != null ? getSSLContextWithClientCertificate() : sslContextFuture;
    }

    private CompletionStage<SSLContext> getSSLContextWithClientCertificate() {
        CompletableFuture<SSLContext> sslContextFuture;
        CompletionStage<SSLContext> sslContextStage = null;
        synchronized (this) {
            if (this.sslContextFuture == null) {
                this.sslContextFuture = new CompletableFuture<>();
                sslContextFuture = this.sslContextFuture;
                var sslContext = this.sslContext;
                var previousThrowable = this.throwable;
                sslContextStage = clientCertificateManager
                        .getClientCertificate()
                        .thenApply(clientCertificate -> {
                            if (clientCertificate != null) {
                                var certificate = (InternalClientCertificate) clientCertificate;
                                try {
                                    var keyManagers = createKeyManagers(certificate);
                                    return sslContextSupplier.get(keyManagers);
                                } catch (Throwable throwable) {
                                    var message = "An error occured while loading client certficate.";
                                    var exception = new ClientException(
                                            GqlStatusError.UNKNOWN.getStatus(),
                                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                                            "N/A",
                                            message,
                                            GqlStatusError.DIAGNOSTIC_RECORD,
                                            throwable);
                                    logger.error("An error occured while loading client certficate.", exception);
                                    throw new CompletionException(exception);
                                }
                            } else {
                                if (previousThrowable != null) {
                                    throw new CompletionException(previousThrowable);
                                } else {
                                    if (sslContext == null) {
                                        var message =
                                                "The initial client certificate returned by the manager must not be null.";
                                        var exception = new ClientException(
                                                GqlStatusError.UNKNOWN.getStatus(),
                                                GqlStatusError.UNKNOWN.getStatusDescription(message),
                                                "N/A",
                                                message,
                                                GqlStatusError.DIAGNOSTIC_RECORD,
                                                null);
                                        logger.error(
                                                "The initial client certificate returned by the manager must not be null.",
                                                exception);
                                        throw new CompletionException(exception);
                                    } else {
                                        return sslContext;
                                    }
                                }
                            }
                        });
            } else {
                sslContextFuture = this.sslContextFuture;
            }
        }

        if (sslContextStage != null) {
            sslContextStage.whenComplete((sslContext, throwable) -> {
                throwable = Futures.completionExceptionCause(throwable);
                synchronized (this) {
                    this.sslContextFuture = null;
                    this.sslContext = sslContext;
                    this.throwable = throwable;
                }
                if (throwable != null) {
                    sslContextFuture.completeExceptionally(throwable);
                } else {
                    sslContextFuture.complete(this.sslContext);
                }
            });
        }

        return sslContextFuture;
    }

    protected KeyManager[] createKeyManagers(InternalClientCertificate clientCertificate)
            throws CertificateException, IOException, KeyException, KeyStoreException, NoSuchAlgorithmException,
                    UnrecoverableKeyException {
        var certificateFactory = CertificateFactory.getInstance("X.509");
        var chain = certificateFactory.generateCertificates(new FileInputStream(clientCertificate.certificate()));

        var password = clientCertificate.password();
        var key = new PemParser(new FileInputStream(clientCertificate.privateKey())).getPrivateKey(password);

        var clientKeyStore = KeyStore.getInstance("JKS");
        var pwdChars = password != null ? password.toCharArray() : "password".toCharArray();
        clientKeyStore.load(null, null);
        clientKeyStore.setKeyEntry("neo4j.javadriver.clientcert.", key, pwdChars, chain.toArray(new Certificate[0]));

        var keyMgrFactory = KeyManagerFactory.getInstance("SunX509");
        keyMgrFactory.init(clientKeyStore, pwdChars);

        return keyMgrFactory.getKeyManagers();
    }
}
