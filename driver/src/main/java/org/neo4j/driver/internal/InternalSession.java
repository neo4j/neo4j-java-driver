/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal;

import java.util.Map;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Session;
import org.neo4j.driver.Statement;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.async.StatementResultCursor;
import org.neo4j.driver.internal.async.ExplicitTransaction;
import org.neo4j.driver.internal.async.NetworkSession;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;

import static java.util.Collections.emptyMap;

public class InternalSession extends AbstractStatementRunner implements Session
{
    private final NetworkSession session;

    public InternalSession( NetworkSession session )
    {
        this.session = session;
    }

    @Override
    public StatementResult run( Statement statement )
    {
        return run( statement, TransactionConfig.empty() );
    }

    @Override
    public StatementResult run( String statement, TransactionConfig config )
    {
        return run( statement, emptyMap(), config );
    }

    @Override
    public StatementResult run( String statement, Map<String,Object> parameters, TransactionConfig config )
    {
        return run( new Statement( statement, parameters ), config );
    }

    @Override
    public StatementResult run( Statement statement, TransactionConfig config )
    {
        StatementResultCursor cursor = Futures.blockingGet( session.runAsync( statement, config, false ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while running query in session" ) );

        // query executed, it is safe to obtain a connection in a blocking way
        Connection connection = Futures.getNow( session.connectionAsync() );
        return new InternalStatementResult( connection, cursor );
    }

    @Override
    public boolean isOpen()
    {
        return session.isOpen();
    }

    @Override
    public void close()
    {
        Futures.blockingGet( session.closeAsync(), () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while closing the session" ) );
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction( TransactionConfig.empty() );
    }

    @Override
    public Transaction beginTransaction( TransactionConfig config )
    {
        ExplicitTransaction tx = Futures.blockingGet( session.beginTransactionAsync( config ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while starting a transaction" ) );
        return new InternalTransaction( tx );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work )
    {
        return readTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return transaction( AccessMode.READ, work, config );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work )
    {
        return writeTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return transaction( AccessMode.WRITE, work, config );
    }

    @Override
    public Bookmark lastBookmark()
    {
        return session.lastBookmark();
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public void reset()
    {
        Futures.blockingGet( session.resetAsync(), () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while resetting the session" ) );
    }

    private <T> T transaction( AccessMode mode, TransactionWork<T> work, TransactionConfig config )
    {
        // use different code path compared to async so that work is executed in the caller thread
        // caller thread will also be the one who sleeps between retries;
        // it is unsafe to execute retries in the event loop threads because this can cause a deadlock
        // event loop thread will bock and wait for itself to read some data
        return session.retryLogic().retry( () -> {
            try ( Transaction tx = beginTransaction( mode, config ) )
            {
                try
                {
                    T result = work.execute( tx );
                    tx.success();
                    return result;
                }
                catch ( Throwable t )
                {
                    // mark transaction for failure if the given unit of work threw exception
                    // this will override any success marks that were made by the unit of work
                    tx.failure();
                    throw t;
                }
            }
        } );
    }

    private Transaction beginTransaction( AccessMode mode, TransactionConfig config )
    {
        ExplicitTransaction tx = Futures.blockingGet( session.beginTransactionAsync( mode, config ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while starting a transaction" ) );
        return new InternalTransaction( tx );
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        // try to get current connection if it has been acquired
        Connection connection = null;
        try
        {
            connection = Futures.getNow( session.connectionAsync() );
        }
        catch ( Throwable ignore )
        {
            // ignore errors because handing interruptions is best effort
        }

        if ( connection != null )
        {
            connection.terminateAndRelease( reason );
        }
    }
}
