/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.neo4j.driver.v1.types.TypeSystem;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.value;

class ExplicitTransaction implements Transaction
{
    private enum State
    {
        /** The transaction is running with no explicit success or failure marked */
        ACTIVE,

        /** Running, user marked for success, meaning it'll value committed */
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

    private final Runnable cleanup;
    private final Connection conn;

    private String bookmark = null;
    private State state = State.ACTIVE;

    ExplicitTransaction( Connection conn, Runnable cleanup )
    {
        this( conn, cleanup, null );
    }

    ExplicitTransaction( Connection conn, Runnable cleanup, String bookmark )
    {
        this.conn = conn;
        this.cleanup = cleanup;

        final Map<String, Value> parameters;
        if ( bookmark == null )
        {
            parameters = emptyMap();
        }
        else
        {
            parameters = singletonMap( "bookmark", value( bookmark ) );
        }
        conn.run( "BEGIN", parameters, Collector.NO_OP );
        conn.pullAll( Collector.NO_OP );
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
            if ( conn != null && conn.isOpen() )
            {
                if ( state == State.MARKED_SUCCESS )
                {
                    conn.run( "COMMIT", Collections.<String, Value>emptyMap(), Collector.NO_OP );
                    conn.pullAll( new BookmarkCollector( this ) );
                    conn.sync();
                    state = State.SUCCEEDED;
                }
                else if ( state == State.MARKED_FAILED || state == State.ACTIVE )
                {
                    conn.run( "ROLLBACK", Collections.<String, Value>emptyMap(), Collector.NO_OP );
                    conn.pullAll( new BookmarkCollector( this ) );
                    conn.sync();
                    state = State.ROLLED_BACK;
                }
            }
        }
        finally
        {
            cleanup.run();
        }
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public StatementResult run( String statementText, Value statementParameters )
    {
        return run( new Statement( statementText, statementParameters ) );
    }

    @Override
    public StatementResult run( String statementText )
    {
        return run( statementText, Values.EmptyMap );
    }

    @Override
    public StatementResult run( String statementText, Map<String,Object> statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value(statementParameters);
        return run( statementText, params );
    }

    @Override
    public StatementResult run( String statementTemplate, Record statementParameters )
    {
        Value params = statementParameters == null ? Values.EmptyMap : value( statementParameters.asMap() );
        return run( statementTemplate, params );
    }

    @Override
    public synchronized StatementResult run( Statement statement )
    {
        ensureNotFailed();

        try
        {
            InternalStatementResult cursor = new InternalStatementResult( conn, this, statement );
            conn.run( statement.text(),
                    statement.parameters().asMap( ofValue() ),
                    cursor.runResponseCollector() );
            conn.pullAll( cursor.pullAllResponseCollector() );
            conn.flush();
            return cursor;
        }
        catch ( Neo4jException e )
        {
            // Failed to send messages to the server probably due to IOException in the socket.
            // So we should stop sending more messages in this transaction
            state = State.FAILED;
            throw e;
        }
    }

    @Override
    public boolean isOpen()
    {
        return state == State.ACTIVE;
    }

    private void ensureNotFailed()
    {
        if ( state == State.FAILED || state == State.MARKED_FAILED || state == State.ROLLED_BACK )
        {
            throw new ClientException(
                "Cannot run more statements in this transaction, because previous statements in the " +
                "transaction has failed and the transaction has been rolled back. Please start a new" +
                " transaction to run another statement."
            );
        }
    }

    @Override
    public TypeSystem typeSystem()
    {
        return InternalTypeSystem.TYPE_SYSTEM;
    }

    public synchronized void markToClose()
    {
        state = State.FAILED;
    }

    public String bookmark()
    {
        return bookmark;
    }

    void setBookmark( String bookmark )
    {
        this.bookmark = bookmark;
    }

}
