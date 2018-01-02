/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Test;

import java.io.IOException;

import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.TransientException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.internal.util.ErrorUtil.isFatal;
import static org.neo4j.driver.internal.util.ErrorUtil.newConnectionTerminatedError;
import static org.neo4j.driver.internal.util.ErrorUtil.newNeo4jError;

public class ErrorUtilTest
{
    @Test
    public void shouldCreateAuthenticationException()
    {
        String code = "Neo.ClientError.Security.Unauthorized";
        String message = "Wrong credentials";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( AuthenticationException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    public void shouldCreateClientException()
    {
        String code = "Neo.ClientError.Transaction.InvalidBookmark";
        String message = "Wrong bookmark";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( ClientException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    public void shouldCreateTransientException()
    {
        String code = "Neo.TransientError.Transaction.DeadlockDetected";
        String message = "Deadlock occurred";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( TransientException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    public void shouldCreateDatabaseException()
    {
        String code = "Neo.DatabaseError.Transaction.TransactionLogError";
        String message = "Failed to write the transaction log";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( DatabaseException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    public void shouldCreateDatabaseExceptionWhenErrorCodeIsWrong()
    {
        String code = "WrongErrorCode";
        String message = "Some really strange error";

        Neo4jException error = newNeo4jError( code, message );

        assertThat( error, instanceOf( DatabaseException.class ) );
        assertEquals( code, error.code() );
        assertEquals( message, error.getMessage() );
    }

    @Test
    public void shouldTreatNotNeo4jExceptionAsFatal()
    {
        assertTrue( isFatal( new IOException( "IO failed!" ) ) );
    }

    @Test
    public void shouldTreatProtocolErrorAsFatal()
    {
        assertTrue( isFatal( new ClientException( "Neo.ClientError.Request.Invalid", "Illegal request" ) ) );
        assertTrue( isFatal( new ClientException( "Neo.ClientError.Request.InvalidFormat", "Wrong format" ) ) );
        assertTrue( isFatal( new ClientException( "Neo.ClientError.Request.TransactionRequired", "No tx!" ) ) );
    }

    @Test
    public void shouldTreatAuthenticationExceptionAsNonFatal()
    {
        assertFalse( isFatal( new AuthenticationException( "Neo.ClientError.Security.Unauthorized", "" ) ) );
    }

    @Test
    public void shouldTreatClientExceptionAsNonFatal()
    {
        assertFalse( isFatal( new ClientException( "Neo.ClientError.Transaction.ConstraintsChanged", "" ) ) );
    }

    @Test
    public void shouldTreatDatabaseExceptionAsFatal()
    {
        assertTrue( isFatal( new ClientException( "Neo.DatabaseError.Schema.ConstraintCreationFailed", "" ) ) );
    }

    @Test
    public void shouldCreateConnectionTerminatedError()
    {
        ServiceUnavailableException error = newConnectionTerminatedError();
        assertThat( error.getMessage(), startsWith( "Connection to the database terminated" ) );
    }

    @Test
    public void shouldCreateConnectionTerminatedErrorWithNullReason()
    {
        ServiceUnavailableException error = newConnectionTerminatedError( null );
        assertThat( error.getMessage(), startsWith( "Connection to the database terminated" ) );
    }

    @Test
    public void shouldCreateConnectionTerminatedErrorWithReason()
    {
        String reason = "Thread interrupted";
        ServiceUnavailableException error = newConnectionTerminatedError( reason );
        assertThat( error.getMessage(), startsWith( "Connection to the database terminated" ) );
        assertThat( error.getMessage(), containsString( reason ) );
    }
}
