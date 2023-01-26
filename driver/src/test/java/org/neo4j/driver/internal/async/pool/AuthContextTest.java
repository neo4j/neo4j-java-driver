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
package org.neo4j.driver.internal.async.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;

class AuthContextTest {
    @Test
    void shouldRejectNullAuthTokenManager() {
        assertThrows(NullPointerException.class, () -> new AuthContext(null));
    }

    @Test
    void shouldStartUnauthenticated() {
        // given
        var authTokenManager = mock(AuthTokenManager.class);

        // when
        var authContext = new AuthContext(authTokenManager);

        // then
        assertEquals(authTokenManager, authContext.getAuthTokenManager());
        assertNull(authContext.getAuthToken());
        assertNull(authContext.getAuthTimestamp());
        assertFalse(authContext.isPendingLogoff());
    }

    @Test
    void shouldInitiateAuth() {
        // given
        var authTokenManager = mock(AuthTokenManager.class);
        var authContext = new AuthContext(authTokenManager);
        var authToken = mock(AuthToken.class);

        // when
        authContext.initiateAuth(authToken);

        // then
        assertEquals(authTokenManager, authContext.getAuthTokenManager());
        assertEquals(authContext.getAuthToken(), authToken);
        assertNull(authContext.getAuthTimestamp());
        assertFalse(authContext.isPendingLogoff());
    }

    @Test
    void shouldRejectNullToken() {
        // given
        var authTokenManager = mock(AuthTokenManager.class);
        var authContext = new AuthContext(authTokenManager);
        var authToken = mock(AuthToken.class);

        // when & then
        assertThrows(NullPointerException.class, () -> authContext.initiateAuth(null));
    }

    @Test
    void shouldInitiateAuthAfterAnotherAuth() {
        // given
        var authTokenManager = mock(AuthTokenManager.class);
        var authContext = new AuthContext(authTokenManager);
        var authToken = mock(AuthToken.class);
        authContext.initiateAuth(mock(AuthToken.class));
        authContext.finishAuth(1L);

        // when
        authContext.initiateAuth(authToken);

        // then
        assertEquals(authTokenManager, authContext.getAuthTokenManager());
        assertEquals(authContext.getAuthToken(), authToken);
        assertNull(authContext.getAuthTimestamp());
        assertFalse(authContext.isPendingLogoff());
    }

    @Test
    void shouldFinishAuth() {
        // given
        var authTokenManager = mock(AuthTokenManager.class);
        var authContext = new AuthContext(authTokenManager);
        var authToken = mock(AuthToken.class);
        authContext.initiateAuth(authToken);
        var ts = 1L;

        // when
        authContext.finishAuth(ts);

        // then
        assertEquals(authTokenManager, authContext.getAuthTokenManager());
        assertEquals(authContext.getAuthToken(), authToken);
        assertEquals(authContext.getAuthTimestamp(), ts);
        assertFalse(authContext.isPendingLogoff());
    }

    @Test
    void shouldSetPendingLogoff() {
        // given
        var authTokenManager = mock(AuthTokenManager.class);
        var authContext = new AuthContext(authTokenManager);
        var authToken = mock(AuthToken.class);
        authContext.initiateAuth(authToken);
        var ts = 1L;
        authContext.finishAuth(ts);

        // when
        authContext.markPendingLogoff();

        // then
        assertEquals(authTokenManager, authContext.getAuthTokenManager());
        assertEquals(authContext.getAuthToken(), authToken);
        assertEquals(authContext.getAuthTimestamp(), ts);
        assertTrue(authContext.isPendingLogoff());
    }
}
