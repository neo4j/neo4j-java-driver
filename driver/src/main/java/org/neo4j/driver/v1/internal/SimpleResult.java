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

import java.util.Iterator;
import java.util.List;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.ResultSummary;
import org.neo4j.driver.v1.ReusableResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

public class SimpleResult implements Result
{
    private final Iterable<String> fieldNames;
    private final List<Record> body;
    private final Iterator<Record> iter;
    private final ResultSummary summary;

    private Record current = null;

    public SimpleResult( Iterable<String> fieldNames, List<Record> body, ResultSummary summary )
    {
        this.fieldNames = fieldNames;
        this.body = body;
        this.iter = body.iterator();
        this.summary = summary;
    }

    @Override
    public ReusableResult retain()
    {
        return new StandardReusableResult( body );
    }

    @Override
    public Record single()
    {
        return iter.next();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public ResultSummary summarize()
    {
        while (next()) ;
        return summary;
    }

    @Override
    public boolean next()
    {
        if ( iter.hasNext() )
        {
            current = iter.next();
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public Value get( int fieldIndex )
    {
        return current.get( fieldIndex );
    }

    @Override
    public Value get( String fieldName )
    {
        if( current == null )
        {
            throw new ClientException(
                    "In order to access fields of a record in a result, " +
                    "you must first call next() to point the result to the next record in the result stream." );
        }
        return current.get( fieldName );
    }

    @Override
    public Iterable<String> fieldNames()
    {
        return fieldNames;
    }

    private static class StandardReusableResult implements ReusableResult
    {
        private final List<Record> body;

        private StandardReusableResult( List<Record> body )
        {
            this.body = body;
        }

        @Override
        public long size()
        {
            return body.size();
        }

        @Override
        public Record get( long index )
        {
            if ( index < 0 || index >= body.size() )
            {
                throw new ClientException( "Value " + index + " does not exist" );
            }
            return body.get( (int) index );
        }

        @Override
        public Iterator<Record> iterator()
        {
            return body.iterator();
        }
    }
}
