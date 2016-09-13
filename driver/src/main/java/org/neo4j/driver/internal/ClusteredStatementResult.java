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

import java.util.List;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ConnectionFailureException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;

public class ClusteredStatementResult implements StatementResult
{
    private final StatementResult delegate;
    private final BoltServerAddress address;
    private final Consumer<BoltServerAddress> onFailedConnection;

    ClusteredStatementResult( StatementResult delegate, BoltServerAddress address, Consumer<BoltServerAddress> onFailedConnection )
    {
        this.delegate = delegate;
        this.address = address;
        this.onFailedConnection = onFailedConnection;
    }

    @Override
    public List<String> keys()
    {
        return delegate.keys();
    }

    @Override
    public boolean hasNext()
    {
        return delegate.hasNext();
    }

    @Override
    public Record next()
    {
        try
        {
            return delegate.next();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e );
        }
    }


    @Override
    public Record single() throws NoSuchRecordException
    {
        try
        {
            return delegate.single();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e );
        }
    }

    @Override
    public Record peek()
    {
        try
        {
            return delegate.peek();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e );
        }
    }

    @Override
    public List<Record> list()
    {
        try
        {
            return delegate.list();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e );
        }
    }

    @Override
    public <T> List<T> list( Function<Record,T> mapFunction )
    {
        try
        {
            return delegate.list(mapFunction);
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e );
        }
    }

    @Override
    public ResultSummary consume()
    {
        try
        {
            return delegate.consume();
        }
        catch ( ConnectionFailureException e )
        {
            throw sessionExpired( e );
        }
    }

    private SessionExpiredException sessionExpired( ConnectionFailureException e )
    {
        onFailedConnection.accept( address );
        return new SessionExpiredException( String.format( "Server at %s is no longer available", address.toString()), e);
    }
}
