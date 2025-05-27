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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.io.File;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.ClientCertificates;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.InternalClientCertificate;

class SSLContextManagerTest {
    private SSLContextManager manager;

    @Test
    void shouldErrorOnInitialNullCertificate() throws NoSuchAlgorithmException, KeyManagementException {
        // GIVEN
        var clientManager = mock(ClientCertificateManager.class);
        given(clientManager.getClientCertificate()).willReturn(CompletableFuture.completedStage(null));
        var contextSupplier = mock(SecurityPlan.SSLContextSupplier.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        given(logging.getLog(any(Class.class))).willReturn(logger);
        manager = new SSLContextManager(clientManager, contextSupplier, logging);
        Consumer<CompletableFuture<SSLContext>> verify = contextFuture -> {
            assertTrue(contextFuture.isCompletedExceptionally());
            var completionException = assertThrows(CompletionException.class, contextFuture::join);
            assertInstanceOf(ClientException.class, completionException.getCause());
        };

        for (var i = 0; i < 5; i++) {
            // WHEN
            var contextFuture = manager.getSSLContext().toCompletableFuture();
            // THEN
            verify.accept(contextFuture);
        }
        then(contextSupplier).shouldHaveNoInteractions();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldAllowNullCertificate() throws NoSuchAlgorithmException, KeyManagementException {
        // GIVEN
        var file = mock(File.class);
        var certificate = ClientCertificates.of(file, file);
        var clientManager = mock(ClientCertificateManager.class);
        given(clientManager.getClientCertificate())
                .willReturn(CompletableFuture.completedStage(certificate), CompletableFuture.completedStage(null));
        var context = mock(SSLContext.class);
        var contextSupplier = mock(SecurityPlan.SSLContextSupplier.class);
        var keyManagers = new KeyManager[0];
        given(contextSupplier.get(keyManagers)).willReturn(context);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        given(logging.getLog(any(Class.class))).willReturn(logger);
        manager = new ExtendedSSLContextManager(clientManager, contextSupplier, logging, ignored -> new KeyManager[0]);
        var context1 = manager.getSSLContext().toCompletableFuture().join();

        // WHEN
        var context2 = manager.getSSLContext().toCompletableFuture().join();

        // THEN
        assertEquals(context1, context2);
        then(clientManager).should(times(2)).getClientCertificate();
        then(contextSupplier).should().get(keyManagers);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldErrorOnCreatingKeyManagers() throws NoSuchAlgorithmException, KeyManagementException {
        // GIVEN
        var file = mock(File.class);
        var certificate = (InternalClientCertificate) ClientCertificates.of(file, file);
        var clientManager = mock(ClientCertificateManager.class);
        given(clientManager.getClientCertificate())
                .willReturn(CompletableFuture.completedStage(certificate), CompletableFuture.completedStage(null));
        var contextSupplier = mock(SecurityPlan.SSLContextSupplier.class);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        given(logging.getLog(any(Class.class))).willReturn(logger);
        Function<InternalClientCertificate, KeyManager[]> keyManagersFunction = mock(Function.class);
        var exception = new RuntimeException();
        given(keyManagersFunction.apply(certificate)).willThrow(exception);
        manager = new ExtendedSSLContextManager(clientManager, contextSupplier, logging, keyManagersFunction);
        Consumer<CompletableFuture<SSLContext>> verify = contextFuture -> {
            assertTrue(contextFuture.isCompletedExceptionally());
            var completionException = assertThrows(CompletionException.class, contextFuture::join);
            assertInstanceOf(ClientException.class, completionException.getCause());
            assertEquals(exception, completionException.getCause().getCause());
        };

        for (var i = 0; i < 2; i++) {
            // WHEN
            var contextFuture = manager.getSSLContext().toCompletableFuture();
            // THEN
            verify.accept(contextFuture);
        }
        then(clientManager).should(times(2)).getClientCertificate();
        then(keyManagersFunction).should().apply(certificate);
        then(contextSupplier).shouldHaveNoInteractions();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldReplaceErrorWithValidContext() throws NoSuchAlgorithmException, KeyManagementException {
        // GIVEN
        var file = mock(File.class);
        var certificate = (InternalClientCertificate) ClientCertificates.of(file, file);
        var clientManager = mock(ClientCertificateManager.class);
        given(clientManager.getClientCertificate()).willReturn(CompletableFuture.completedStage(certificate));
        var context = mock(SSLContext.class);
        var contextSupplier = mock(SecurityPlan.SSLContextSupplier.class);
        given(contextSupplier.get(any())).willReturn(context);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        given(logging.getLog(any(Class.class))).willReturn(logger);
        Function<InternalClientCertificate, KeyManager[]> keyManagersFunction = mock(Function.class);
        var exception = new RuntimeException();
        var callNum = new AtomicInteger();
        var keyManagers = new KeyManager[0];
        given(keyManagersFunction.apply(certificate)).willAnswer(invocation -> {
            if (callNum.get() > 0) {
                return keyManagers;
            } else {
                callNum.getAndIncrement();
                throw exception;
            }
        });
        manager = new ExtendedSSLContextManager(clientManager, contextSupplier, logging, keyManagersFunction);
        var failedContextFuture = manager.getSSLContext().toCompletableFuture();
        assertTrue(failedContextFuture.isCompletedExceptionally());

        // WHEN
        var actualContext = manager.getSSLContext().toCompletableFuture().join();

        // THEN
        assertEquals(context, actualContext);
        then(clientManager).should(times(2)).getClientCertificate();
        then(keyManagersFunction).should(times(2)).apply(certificate);
        then(contextSupplier).should().get(keyManagers);
    }

    @Test
    void shouldAcceptNullCertificateManager() throws NoSuchAlgorithmException, KeyManagementException {
        // GIVEN
        var context = mock(SSLContext.class);
        var contextSupplier = mock(SecurityPlan.SSLContextSupplier.class);
        var keyManagers = new KeyManager[0];
        given(contextSupplier.get(keyManagers)).willReturn(context);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var logger = mock(Logger.class);
        given(logging.getLog(any(Class.class))).willReturn(logger);
        manager = new SSLContextManager(null, contextSupplier, logging);

        var actualContext = manager.getSSLContext().toCompletableFuture().join();

        assertEquals(context, actualContext);
        then(contextSupplier).should().get(keyManagers);
    }

    private static class ExtendedSSLContextManager extends SSLContextManager {
        private final Function<InternalClientCertificate, KeyManager[]> keyManagersFunction;

        public ExtendedSSLContextManager(
                ClientCertificateManager clientCertificateManager,
                SecurityPlan.SSLContextSupplier sslContextSupplier,
                @SuppressWarnings("deprecation") Logging logging,
                Function<InternalClientCertificate, KeyManager[]> keyManagersFunction)
                throws NoSuchAlgorithmException, KeyManagementException {
            super(clientCertificateManager, sslContextSupplier, logging);
            this.keyManagersFunction = keyManagersFunction;
        }

        @Override
        protected KeyManager[] createKeyManagers(InternalClientCertificate clientCertificate) {
            return keyManagersFunction.apply(clientCertificate);
        }
    }
}
