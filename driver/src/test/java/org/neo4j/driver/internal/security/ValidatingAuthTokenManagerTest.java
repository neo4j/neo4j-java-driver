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
import org.neo4j.driver.exceptions.TokenExpiredException;

class ValidatingAuthTokenManagerTest {
    private static final TokenExpiredException TOKEN_EXPIRED_EXCEPTION = new TokenExpiredException("code", "message");

    @Test
    void shouldReturnFailedStageOnInvalidAuthTokenType() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        given(delegateManager.getToken()).willReturn(completedFuture(null));
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
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());

        // when
        var tokenFuture = manager.getToken().toCompletableFuture();

        // then
        assertEquals(token, tokenFuture.join());
    }

    @Test
    void shouldRejectNullAuthTokenOnExpiration() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());

        // when & then
        assertThrows(NullPointerException.class, () -> manager.onSecurityException(null, TOKEN_EXPIRED_EXCEPTION));
        then(delegateManager).shouldHaveNoInteractions();
    }

    @Test
    void shouldPassOriginalTokenOnExpiration() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        var manager = new ValidatingAuthTokenManager(delegateManager, Logging.none());
        var token = AuthTokens.none();

        // when
        manager.onSecurityException(token, TOKEN_EXPIRED_EXCEPTION);

        // then
        then(delegateManager).should().onSecurityException(token, TOKEN_EXPIRED_EXCEPTION);
    }

    @Test
    void shouldLogWhenDelegateOnExpiredFails() {
        // given
        var delegateManager = mock(AuthTokenManager.class);
        var token = AuthTokens.none();
        var exception = mock(RuntimeException.class);
        willThrow(exception).given(delegateManager).onSecurityException(token, TOKEN_EXPIRED_EXCEPTION);
        var logging = mock(Logging.class);
        var log = mock(Logger.class);
        given(logging.getLog(ValidatingAuthTokenManager.class)).willReturn(log);
        var manager = new ValidatingAuthTokenManager(delegateManager, logging);

        // when
        manager.onSecurityException(token, TOKEN_EXPIRED_EXCEPTION);

        // then
        then(delegateManager).should().onSecurityException(token, TOKEN_EXPIRED_EXCEPTION);
        then(log).should().warn(anyString());
        then(log).should().debug(anyString(), eq(exception));
    }
}
