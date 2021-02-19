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
package org.neo4j.driver.internal.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.util.ErrorUtil.isFatal;
import static org.neo4j.driver.internal.util.ErrorUtil.newConnectionTerminatedError;
import static org.neo4j.driver.internal.util.ErrorUtil.newNeo4jError;

class ErrorUtilTest
{
    @Test
    void shouldCreateAuthenticationException()
    {
        String code = "Neo.ClientError.Security.Unauthorized";
        String message = "Wrong credentials";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( AuthenticationException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    void shouldCreateClientException()
    {
        String code = "Neo.ClientError.Transaction.InvalidBookmark";
        String message = "Wrong bookmark";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( ClientException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    void shouldCreateTransientException()
    {
        String code = "Neo.TransientError.Transaction.DeadlockDetected";
        String message = "Deadlock occurred";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( TransientException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    void shouldCreateDatabaseException()
    {
        String code = "Neo.DatabaseError.Transaction.TransactionLogError";
        String message = "Failed to write the transaction log";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( DatabaseException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    void shouldCreateDatabaseExceptionWhenErrorCodeIsWrong()
    {
        String code = "WrongErrorCode";
        String message = "Some really strange error";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( DatabaseException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    void shouldTreatNotNeo4jExceptionAsFatal()
    {
        assertTrue( isFatal( new IOException( "IO failed!" ) ) );
    }

    @Test
    void shouldTreatProtocolErrorAsFatal()
    {
        assertTrue( isFatal( new ClientException( "Neo.ClientError.Request.Invalid", "Illegal request" ) ) );
        assertTrue( isFatal( new ClientException( "Neo.ClientError.Request.InvalidFormat", "Wrong format" ) ) );
        assertTrue( isFatal( new ClientException( "Neo.ClientError.Request.TransactionRequired", "No tx!" ) ) );
    }

    @Test
    void shouldTreatAuthenticationExceptionAsNonFatal()
    {
        assertFalse( isFatal( new AuthenticationException( "Neo.ClientError.Security.Unauthorized", "" ) ) );
    }

    @Test
    void shouldTreatClientExceptionAsNonFatal()
    {
        assertFalse( isFatal( new ClientException( "Neo.ClientError.Transaction.ConstraintsChanged", "" ) ) );
    }

    @Test
    void shouldTreatDatabaseExceptionAsFatal()
    {
        assertTrue( isFatal( new ClientException( "Neo.DatabaseError.Schema.ConstraintCreationFailed", "" ) ) );
    }

    @Test
    void shouldCreateConnectionTerminatedError()
    {
        ServiceUnavailableException error = newConnectionTerminatedError();
        assertThat( error.getMessage(), startsWith( "Connection to the database terminated" ) );
    }

    @Test
    void shouldCreateConnectionTerminatedErrorWithNullReason()
    {
        ServiceUnavailableException error = newConnectionTerminatedError( null );
        assertThat( error.getMessage(), startsWith( "Connection to the database terminated" ) );
    }

    @Test
    void shouldCreateConnectionTerminatedErrorWithReason()
    {
        String reason = "Thread interrupted";
        ServiceUnavailableException error = newConnectionTerminatedError( reason );
        assertThat( error.getMessage(), startsWith( "Connection to the database terminated" ) );
        assertThat( error.getMessage(), containsString( reason ) );
    }
}
