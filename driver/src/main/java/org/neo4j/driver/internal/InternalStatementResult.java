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

import java.util.List;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;

import static org.neo4j.driver.internal.async.Futures.getBlocking;

public class InternalStatementResult implements StatementResult
{
    private final StatementResultCursor cursor;
    private List<String> keys;

    public InternalStatementResult( StatementResultCursor cursor )
    {
        this.cursor = cursor;
    }

    @Override
    public List<String> keys()
    {
        if ( keys == null )
        {
            getBlocking( cursor.peekAsync() );
            keys = cursor.keys();
        }
        return keys;
    }

    @Override
    public boolean hasNext()
    {
        return getBlocking( cursor.peekAsync() ) != null;
    }

    @Override
    public Record next()
    {
        Record record = getBlocking( cursor.nextAsync() );
        if ( record == null )
        {
            throw new NoSuchRecordException( "No more records" );
        }
        return record;
    }

    @Override
    public Record single()
    {
        return getBlocking( cursor.singleAsync() );
    }

    @Override
    public Record peek()
    {
        Record record = getBlocking( cursor.peekAsync() );
        if ( record == null )
        {
            throw new NoSuchRecordException( "Cannot peek past the last record" );
        }
        return record;
    }

    @Override
    public List<Record> list()
    {
        return getBlocking( cursor.listAsync() );
    }

    @Override
    public <T> List<T> list( Function<Record, T> mapFunction )
    {
        return getBlocking( cursor.listAsync( mapFunction ) );
    }

    @Override
    public ResultSummary consume()
    {
        return getBlocking( cursor.consumeAsync() );
    }

    @Override
    public ResultSummary summary()
    {
        return getBlocking( cursor.summaryAsync() );
    }

    @Override
    public void remove()
    {
        throw new ClientException( "Removing records from a result is not supported." );
    }
}
