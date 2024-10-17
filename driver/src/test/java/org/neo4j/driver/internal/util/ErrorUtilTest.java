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
package org.neo4j.driver.internal.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.util.ErrorUtil.isFatal;
import static org.neo4j.driver.internal.util.ErrorUtil.newConnectionTerminatedError;
import static org.neo4j.driver.internal.util.ErrorUtil.newNeo4jError;
import static org.neo4j.driver.internal.util.ErrorUtil.rethrowAsyncException;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TokenExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.bolt.api.GqlError;

class ErrorUtilTest {
    @Test
    void shouldCreateAuthenticationException() {
        var code = "Neo.ClientError.Security.Unauthorized";
        var message = "Wrong credentials";

        var error = newNeo4jError(new GqlError(code, message));

        assertThat(error, instanceOf(AuthenticationException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateClientException() {
        var code = "Neo.ClientError.Transaction.InvalidBookmark";
        var message = "Wrong bookmark";

        var error = newNeo4jError(new GqlError(code, message));

        assertThat(error, instanceOf(ClientException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateTransientException() {
        var code = "Neo.TransientError.Transaction.DeadlockDetected";
        var message = "Deadlock occurred";

        var error = newNeo4jError(new GqlError(code, message));

        assertThat(error, instanceOf(TransientException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateDatabaseException() {
        var code = "Neo.DatabaseError.Transaction.TransactionLogError";
        var message = "Failed to write the transaction log";

        var error = newNeo4jError(new GqlError(code, message));

        assertThat(error, instanceOf(DatabaseException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateDatabaseExceptionWhenErrorCodeIsWrong() {
        var code = "WrongErrorCode";
        var message = "Some really strange error";

        var error = newNeo4jError(new GqlError(code, message));

        assertThat(error, instanceOf(DatabaseException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldTreatNotNeo4jExceptionAsFatal() {
        assertTrue(isFatal(new IOException("IO failed!")));
    }

    @Test
    void shouldTreatProtocolErrorAsFatal() {
        assertTrue(isFatal(new ClientException("Neo.ClientError.Request.Invalid", "Illegal request")));
        assertTrue(isFatal(new ClientException("Neo.ClientError.Request.InvalidFormat", "Wrong format")));
        assertTrue(isFatal(new ClientException("Neo.ClientError.Request.TransactionRequired", "No tx!")));
    }

    @Test
    void shouldTreatAuthenticationExceptionAsNonFatal() {
        assertFalse(isFatal(new AuthenticationException("Neo.ClientError.Security.Unauthorized", "")));
    }

    @Test
    void shouldTreatClientExceptionAsNonFatal() {
        assertFalse(isFatal(new ClientException("Neo.ClientError.Transaction.ConstraintsChanged", "")));
    }

    @Test
    void shouldTreatDatabaseExceptionAsFatal() {
        assertTrue(isFatal(new ClientException("Neo.DatabaseError.Schema.ConstraintCreationFailed", "")));
    }

    @Test
    void shouldCreateConnectionTerminatedError() {
        var error = newConnectionTerminatedError();
        assertThat(error.getMessage(), startsWith("Connection to the database terminated"));
    }

    @Test
    void shouldCreateConnectionTerminatedErrorWithNullReason() {
        var error = newConnectionTerminatedError(null);
        assertThat(error.getMessage(), startsWith("Connection to the database terminated"));
    }

    @Test
    void shouldCreateConnectionTerminatedErrorWithReason() {
        var reason = "Thread interrupted";
        var error = newConnectionTerminatedError(reason);
        assertThat(error.getMessage(), startsWith("Connection to the database terminated"));
        assertThat(error.getMessage(), containsString(reason));
    }

    @Test
    void shouldCreateAuthorizationExpiredException() {
        var code = "Neo.ClientError.Security.AuthorizationExpired";
        var message = "Expired authorization info";

        var error = newNeo4jError(new GqlError(code, message));

        assertThat(error, instanceOf(AuthorizationExpiredException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldCreateTokenExpiredException() {
        var code = "Neo.ClientError.Security.TokenExpired";
        var message = "message";

        var error = newNeo4jError(new GqlError(code, message));

        assertThat(error, instanceOf(TokenExpiredException.class));
        assertEquals(code, error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldMapTransientTransactionTerminatedToClientException() {
        var code = "Neo.TransientError.Transaction.Terminated";
        var message = "message";

        var error = newNeo4jError(new GqlError(code, message));

        assertThat(error, instanceOf(ClientException.class));
        assertEquals("Neo.ClientError.Transaction.Terminated", error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldMapTransientTransactionLockClientStoppedToClientException() {
        var code = "Neo.TransientError.Transaction.LockClientStopped";
        var message = "message";

        var error = newNeo4jError(new GqlError(code, message));

        assertThat(error, instanceOf(ClientException.class));
        assertEquals("Neo.ClientError.Transaction.LockClientStopped", error.code());
        assertEquals(message, error.getMessage());
    }

    @Test
    void shouldWrapCheckedExceptionsInNeo4jExceptionWhenRethrowingAsyncException() {
        var ee = mock(ExecutionException.class);
        var uhe = mock(UnknownHostException.class);
        given(ee.getCause()).willReturn(uhe);
        given(uhe.getStackTrace()).willReturn(new StackTraceElement[0]);

        var actual = assertThrows(Neo4jException.class, () -> rethrowAsyncException(ee));

        assertEquals(actual.getCause(), uhe);
    }
}
