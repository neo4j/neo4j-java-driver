/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.internal.spi.Connection;
import org.neo4j.driver.v1.internal.summary.ResultBuilder;

public class StandardTransaction implements Transaction
{
    private final Connection conn;
    private final Runnable cleanup;

    private enum State
    {
        /** The transaction is running with no explicit success or failure marked */
        ACTIVE,

        /** Running, user marked for success, meaning it'll get committed */
        MARKED_SUCCESS,

        /** User marked as failed, meaning it'll be rolled back. */
        MARKED_FAILED,

        /**
         * An error has occurred, transaction can no longer be used and no more messages will be sent for this
         * transaction.
         */
        FAILED,

        /** This transaction has successfully committed */
        SUCCEEDED,

        /** This transaction has been rolled back */
        ROLLED_BACK
    }

    private State state = State.ACTIVE;

    public StandardTransaction( Connection conn, Runnable cleanup )
    {
        this.conn = conn;
        this.cleanup = cleanup;

        // Note there is no sync here, so this will just get queued locally
        conn.run( "BEGIN", Collections.<String, Value>emptyMap(), null );
        conn.discardAll();
    }

    @Override
    public void success()
    {
        if ( state == State.ACTIVE )
        {
            state = State.MARKED_SUCCESS;
        }
    }

    @Override
    public void failure()
    {
        if ( state == State.ACTIVE || state == State.MARKED_SUCCESS )
        {
            state = State.MARKED_FAILED;
        }
    }

    @Override
    public void close()
    {
        try
        {
            if ( state == State.MARKED_SUCCESS )
            {
                conn.run( "COMMIT", Collections.<String, Value>emptyMap(), null );
                conn.discardAll();
                conn.sync();
                state = State.SUCCEEDED;
            }
            else if ( state == State.MARKED_FAILED || state == State.ACTIVE )
            {
                // If alwaysValid of the things we've put in the queue have been sent off, there is no need to
                // do this, we could just clear the queue. Future optimization.
                conn.run( "ROLLBACK", Collections.<String, Value>emptyMap(), null );
                conn.discardAll();
                state = State.ROLLED_BACK;
            }
        }
        finally
        {
            cleanup.run();
        }
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public Result run( String statementText, Map<String,Value> parameters )
    {
        ensureNotFailed();

        try
        {
            ResultBuilder resultBuilder = new ResultBuilder( statementText, parameters );
            conn.run( statementText, parameters, resultBuilder );
            conn.pullAll( resultBuilder );
            conn.sync();
            return resultBuilder.build();
        }
        catch ( Neo4jException e )
        {
            state = State.FAILED;
            throw e;
        }
    }

    @Override
    public Result run( String statementText )
    {
        return run( statementText, ParameterSupport.NO_PARAMETERS );
    }

    @Override
    public Result run( Statement statement )
    {
        return run( statement.text(), statement.parameters() );
    }

    @Override
    public boolean isOpen()
    {
        return state == State.ACTIVE;
    }

    private void ensureNotFailed()
    {
        if ( state == State.FAILED )
        {
            throw new ClientException(
                "Cannot run more statements in this transaction, because previous statements in the " +
                "transaction has failed and the transaction has been rolled back. Please start a new" +
                " transaction to run another statement."
            );
        }
    }
}
