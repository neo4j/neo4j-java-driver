/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.ResultResourcesHandler;
import org.neo4j.driver.internal.handlers.RecordsResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.internal.summary.InternalResultSummary;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Functions;

import static java.util.Collections.emptyList;

public class InternalStatementResult implements StatementResult
{
    private final Statement statement;
    private final Connection connection;
    private final ResultResourcesHandler resourcesHandler;
    private final RunResponseHandler runResponseHandler;
    private final RecordsResponseHandler pullAllResponseHandler;

    InternalStatementResult( Statement statement, Connection connection, ResultResourcesHandler resourcesHandler )
    {
        this.statement = statement;
        this.connection = connection;
        this.runResponseHandler = new RunResponseHandler();
        this.pullAllResponseHandler = new RecordsResponseHandler( runResponseHandler );
        this.resourcesHandler = resourcesHandler;
    }

    ResponseHandler runResponseHandler()
    {
        return runResponseHandler;
    }

    ResponseHandler pullAllResponseHandler()
    {
        return pullAllResponseHandler;
    }

    @Override
    public List<String> keys()
    {
        if ( runResponseHandler.statementKeys() == null )
        {
            tryFetchNext();
        }
        return runResponseHandler.statementKeys();
    }

    @Override
    public boolean hasNext()
    {
        return tryFetchNext();
    }

    @Override
    public Record next()
    {
        if ( tryFetchNext() )
        {
            return pullAllResponseHandler.recordBuffer().poll();
        }
        else
        {
            throw new NoSuchRecordException( "No more records" );
        }
    }

    @Override
    public Record single()
    {
        if ( hasNext() )
        {
            Record single = next();
            boolean hasMoreThanOne = hasNext();

            consume();

            if ( hasMoreThanOne )
            {
                throw new NoSuchRecordException( "Expected a result with a single record, but this result contains at least one more. " +
                        "Ensure your query returns only one record, or use `first` instead of `single` if " +
                        "you do not care about the number of records in the result." );
            }

            return single;
        }
        else
        {
            throw new NoSuchRecordException( "Cannot retrieve a single record, because this result is empty." );
        }
    }

    @Override
    public Record peek()
    {
        if ( tryFetchNext() )
        {
            return pullAllResponseHandler.recordBuffer().peek();
        }
        else
        {
            throw new NoSuchRecordException( "Cannot peek past the last record" );
        }
    }

    @Override
    public List<Record> list()
    {
        return list( Functions.<Record>identity() );
    }

    @Override
    public <T> List<T> list( Function<Record, T> mapFunction )
    {
        if ( hasNext() )
        {
            List<T> result = new ArrayList<>();

            do
            {
                result.add( mapFunction.apply( next() ) );
            }
            while ( hasNext() );

            return result;
        }
        else
        {
            return emptyList();
        }
    }

    @Override
    public ResultSummary consume()
    {
        if ( pullAllResponseHandler.isCompleted() )
        {
            pullAllResponseHandler.recordBuffer().clear();
        }
        else
        {
            do
            {
                receiveOne();
                pullAllResponseHandler.recordBuffer().clear();
            }
            while ( !pullAllResponseHandler.isCompleted() );
        }

        return createResultSummary();
    }

    @Override
    public ResultSummary summary()
    {
        while ( !pullAllResponseHandler.isCompleted() )
        {
            receiveOne();
        }

        return createResultSummary();
    }

    @Override
    public void remove()
    {
        throw new ClientException( "Removing records from a result is not supported." );
    }

    private boolean tryFetchNext()
    {
        while ( pullAllResponseHandler.recordBuffer().isEmpty() )
        {
            if ( pullAllResponseHandler.isCompleted() )
            {
                return false;
            }
            receiveOne();
        }

        return true;
    }

    private void receiveOne()
    {
        try
        {
            connection.receiveOne();
        }
        catch ( Throwable error )
        {
            resourcesHandler.resultFailed( error );
            throw error;
        }
        if ( pullAllResponseHandler.isCompleted() )
        {
            resourcesHandler.resultFetched();
        }
    }

    private ResultSummary createResultSummary()
    {
        return new InternalResultSummary(
                statement,
                connection.server(),
                pullAllResponseHandler.statementType(),
                pullAllResponseHandler.counters(),
                pullAllResponseHandler.plan(),
                pullAllResponseHandler.profile(),
                pullAllResponseHandler.notifications(),
                runResponseHandler.resultAvailableAfter(),
                pullAllResponseHandler.resultConsumedAfter()
        );
    }
}
