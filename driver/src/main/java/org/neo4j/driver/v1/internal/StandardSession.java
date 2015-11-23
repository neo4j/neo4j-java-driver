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

import java.util.Map;

import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.internal.spi.Connection;
import org.neo4j.driver.v1.internal.summary.ResultBuilder;

public class StandardSession implements Session
{
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
    private boolean isOpen = true;

    public StandardSession( Connection connection )
    {
        this.connection = connection;
    }

    @Override
    public Result run( String statementText, Map<String,Value> parameters )
    {
        ensureNoOpenTransaction();
        ResultBuilder resultBuilder = new ResultBuilder( statementText, parameters );
        connection.run( statementText, parameters, resultBuilder );

        connection.pullAll( resultBuilder );
        connection.sync();
        return resultBuilder.build();
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
        return isOpen;
    }

    @Override
    public void close()
    {
        if( !isOpen )
        {
            throw new ClientException( "This session has already been closed." );
        }
        else
        {
            isOpen = false;
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
    }

    @Override
    public Transaction beginTransaction()
    {
        ensureNoOpenTransaction();
        return currentTransaction = new StandardTransaction( connection, txCleanup );
    }

    private void ensureNoOpenTransaction()
    {
        if ( currentTransaction != null )
        {
            throw new ClientException( "Please close the currently open transaction object before running " +
                                       "more statements/transactions in the current session." );
        }
    }
}
