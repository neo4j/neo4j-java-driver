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
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;

public class CursorBasedStatementResult implements StatementResult
{
    private final StatementResultCursor cursor;

    public CursorBasedStatementResult( StatementResultCursor cursor )
    {
        this.cursor = cursor;
    }

    @Override
    public List<String> keys()
    {
        return null;
    }

    @Override
    public boolean hasNext()
    {
        return false;
    }

    @Override
    public Record next()
    {
        return null;
    }

    @Override
    public Record single() throws NoSuchRecordException
    {
        return null;
    }

    @Override
    public Record peek()
    {
        return null;
    }

    @Override
    public List<Record> list()
    {
        return null;
    }

    @Override
    public <T> List<T> list( Function<Record,T> mapFunction )
    {
        return null;
    }

    @Override
    public ResultSummary consume()
    {
        return null;
    }

    @Override
    public ResultSummary summary()
    {
        return null;
    }
}
