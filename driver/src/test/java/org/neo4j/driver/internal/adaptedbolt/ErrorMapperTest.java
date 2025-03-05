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
package org.neo4j.driver.internal.adaptedbolt;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Collections;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.connection.GqlStatusError;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.TokenExpiredException;
import org.neo4j.driver.exceptions.TransientException;

class ErrorMapperTest {
    private BoltFailureException newError(String code, String message) {
        return new BoltFailureException(
                code,
                message,
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                Collections.emptyMap(),
                null);
    }

    @Test
    void shouldCreateAuthenticationException() {
        var code = "Neo.ClientError.Security.Unauthorized";
        var message = "Wrong credentials";

        var error = (Neo4jException) ErrorMapper.getInstance().map(newError(code, message));

        assertThat(error, instanceOf(AuthenticationException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateClientException() {
        var code = "Neo.ClientError.Transaction.InvalidBookmark";
        var message = "Wrong bookmark";

        var error = (Neo4jException) ErrorMapper.getInstance().map(newError(code, message));

        assertThat(error, instanceOf(ClientException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateTransientException() {
        var code = "Neo.TransientError.Transaction.DeadlockDetected";
        var message = "Deadlock occurred";

        var error = (Neo4jException) ErrorMapper.getInstance().map(newError(code, message));

        assertThat(error, instanceOf(TransientException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateDatabaseException() {
        var code = "Neo.DatabaseError.Transaction.TransactionLogError";
        var message = "Failed to write the transaction log";

        var error = (Neo4jException) ErrorMapper.getInstance().map(newError(code, message));

        assertThat(error, instanceOf(DatabaseException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateDatabaseExceptionWhenErrorCodeIsWrong() {
        var code = "WrongErrorCode";
        var message = "Some really strange error";

        var error = (Neo4jException) ErrorMapper.getInstance().map(newError(code, message));

        assertThat(error, instanceOf(DatabaseException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateAuthorizationExpiredException() {
        var code = "Neo.ClientError.Security.AuthorizationExpired";
        var message = "Expired authorization info";

        var error = (Neo4jException) ErrorMapper.getInstance().map(newError(code, message));

        assertThat(error, instanceOf(AuthorizationExpiredException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateTokenExpiredException() {
        var code = "Neo.ClientError.Security.TokenExpired";
        var message = "message";

        var error = (Neo4jException) ErrorMapper.getInstance().map(newError(code, message));

        assertThat(error, instanceOf(TokenExpiredException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldMapTransientTransactionTerminatedToClientException() {
        var code = "Neo.TransientError.Transaction.Terminated";
        var message = "message";

        var error = (Neo4jException) ErrorMapper.getInstance().map(newError(code, message));

        assertThat(error, instanceOf(ClientException.class));
        assertEquals("Neo.ClientError.Transaction.Terminated", error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldMapTransientTransactionLockClientStoppedToClientException() {
        var code = "Neo.TransientError.Transaction.LockClientStopped";
        var message = "message";

        var error = (Neo4jException) ErrorMapper.getInstance().map(newError(code, message));

        assertThat(error, instanceOf(ClientException.class));
        assertEquals("Neo.ClientError.Transaction.LockClientStopped", error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldTranslateSSLHandshakeException() {
        var sslHandshakeException = new SSLHandshakeException("Invalid certificate");

        var error = (Neo4jException) ErrorMapper.getInstance().map(sslHandshakeException);

        assertInstanceOf(SecurityException.class, error);
        assertEquals(sslHandshakeException, error.getCause());
    }
}
