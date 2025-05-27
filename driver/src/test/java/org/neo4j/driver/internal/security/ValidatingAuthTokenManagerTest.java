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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.AuthTokenManagerExecutionException;
import org.neo4j.driver.exceptions.SecurityException;

class ValidatingAuthTokenManagerTest {
    @Test
    void shouldReturnFailedStageOnInvalidAuthTokenType() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        given(delegateManager.getToken()).willReturn(completedFuture(null));
        @SuppressWarnings("deprecation")
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());

        // when
        var tokenFuture = manager.getToken().toCompletableFuture();

        // then
        var exception = assertThrows(CompletionException.class, tokenFuture::join);
        assertTrue(exception.getCause() instanceof AuthTokenManagerExecutionException);
        assertTrue(exception.getCause().getCause() instanceof NullPointerException);
    }

    @Test
    void shouldReturnHandleAndWrapDelegateFailure() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        var exception = mock(RuntimeException.class);
        given(delegateManager.getToken()).willThrow(exception);
        @SuppressWarnings("deprecation")
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());

        // when
        var tokenFuture = manager.getToken().toCompletableFuture();

        // then
        var actualException = assertThrows(CompletionException.class, tokenFuture::join);
        assertTrue(actualException.getCause() instanceof AuthTokenManagerExecutionException);
        assertEquals(exception, actualException.getCause().getCause());
    }

    @Test
    void shouldReturnHandleNullTokenStage() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        given(delegateManager.getToken()).willReturn(null);
        @SuppressWarnings("deprecation")
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());

        // when
        var tokenFuture = manager.getToken().toCompletableFuture();

        // then
        var actualException = assertThrows(CompletionException.class, tokenFuture::join);
        assertTrue(actualException.getCause() instanceof AuthTokenManagerExecutionException);
        assertTrue(actualException.getCause().getCause() instanceof NullPointerException);
    }

    @Test
    void shouldPassOriginalToken() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        var token = AuthTokens.none();
        given(delegateManager.getToken()).willReturn(completedFuture(token));
        @SuppressWarnings("deprecation")
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());

        // when
        var tokenFuture = manager.getToken().toCompletableFuture();

        // then
        assertEquals(token, tokenFuture.join());
    }

    @Test
    void shouldRejectNullAuthTokenOnHandleSecurityException() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        @SuppressWarnings("deprecation")
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());

        // when & then
        assertThrows(
                NullPointerException.class, () -> manager.handleSecurityException(null, mock(SecurityException.class)));
        then(delegateManager).shouldHaveNoInteractions();
    }

    @Test
    void shouldRejectNullExceptionOnHandleSecurityException() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        @SuppressWarnings("deprecation")
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());

        // when & then
        assertThrows(NullPointerException.class, () -> manager.handleSecurityException(AuthTokens.none(), null));
        then(delegateManager).shouldHaveNoInteractions();
    }

    @Test
    void shouldPassOriginalTokenAndExceptionOnHandleSecurityException() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        @SuppressWarnings("deprecation")
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());
        var token = AuthTokens.none();
        var exception = mock(SecurityException.class);

        // when
        manager.handleSecurityException(token, exception);

        // then
        then(delegateManager).should().handleSecurityException(token, exception);
    }

    @Test
    void shouldLogWhenDelegateHandleSecurityExceptionFails() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        var token = AuthTokens.none();
        var securityException = mock(SecurityException.class);
        var exception = mock(RuntimeException.class);
        willThrow(exception).given(delegateManager).handleSecurityException(token, securityException);
        @SuppressWarnings("deprecation")
        var logging = mock(Logging.class);
        @SuppressWarnings("deprecation")
        var log = mock(Logger.class);
        given(logging.getLog(ValidatingAuthTokenManager.class)).willReturn(log);
        var manager = new ValidatingAuthTokenManager(delegateManager, logging);

        // when
        manager.handleSecurityException(token, securityException);

        // then
        then(delegateManager).should().handleSecurityException(token, securityException);
        then(log).should().warn(anyString());
        then(log).should().debug(anyString(), eq(exception));
    }
}
