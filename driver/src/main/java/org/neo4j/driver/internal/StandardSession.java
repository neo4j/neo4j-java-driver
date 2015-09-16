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
package org.neo4j.driver.internal;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.spi.Connection;

public class StandardSession implements Session
{
    public static final Map<String,Value> NO_PARAMETERS = new HashMap<>();

    private final Connection connection;

    /** Called when a transaction object is closed */
    private final Runnable txCleanup = new Runnable()
    {
        @Override
        public void run()
        {
            currentTransaction = null;
        }
    };

    private Transaction currentTransaction;

    public StandardSession( Connection connection )
    {
        this.connection = connection;
    }

    @Override
    public Result run( String statement, Map<String,Value> parameters )
    {
        ensureNoOpenTransaction();
        ResultBuilder resultBuilder = new ResultBuilder();
        CombinedResultBuilder combinedResultBuilder = new CombinedResultBuilder( resultBuilder );
        connection.run( statement, parameters, resultBuilder );
        connection.pullAll( combinedResultBuilder );
        connection.sync();
        return combinedResultBuilder.build();
    }

    @Override
    public Result run( String statement )
    {
        return run( statement, NO_PARAMETERS );
    }

    @Override
    public void close()
    {
        if ( currentTransaction != null )
        {
            try
            {
                currentTransaction.close();
            }
            catch ( Throwable e )
            {
                // Best-effort
            }
        }
        connection.close();
    }

    @Override
    public Transaction newTransaction()
    {
        ensureNoOpenTransaction();
        return currentTransaction = new StandardTransaction( connection, txCleanup );
    }

    private void ensureNoOpenTransaction()
    {
        if ( currentTransaction != null )
        {
            throw new ClientException( "Please close the currently open transaction object before running " +
                                       "more statements on the session level." );
        }
    }
}
