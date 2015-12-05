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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Result;
import org.neo4j.driver.v1.ResultSummary;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

public class SimpleResult extends SimpleRecordAccessor implements Result
{
    private final List<String> keys;
    private final Iterator<Record> iter;
    private final ResultSummary summary;

    private boolean open = true;
    private Record current = null;
    private long position = -1;

    public SimpleResult( List<String> keys, List<Record> body, ResultSummary summary )
    {
        this.keys = keys;
        this.iter = body.iterator();
        this.summary = summary;
    }

    @Override
    public boolean isOpen()
    {
        return open;
    }

    public Value value( int index )
    {
        return current == null ? throwNoRecord() : current.value( index );
    }

    @Override
    public boolean containsKey( String key )
    {
        return keys.contains( key );
    }

    public List<String> keys()
    {
        return keys;
    }

    public Value value( String key )
    {
        assertOpen();
        return current == null ? throwNoRecord() : current.value( key );
    }

    private Value throwNoRecord()
    {
        throw new ClientException(
            "In order to access fields of a record in a result, " +
            "you must first call next() to point the result to the next record in the result stream."
        );
    }

    @Override
    public Record record()
    {
        assertOpen();
        if ( current == null )
        {
            throwNoRecord();
        }
        return current;
    }

    @Override
    public long position()
    {
        assertOpen();

        return position;
    }

    @Override
    public boolean atEnd()
    {
        assertOpen();

        return !iter.hasNext();
    }

    @Override
    public boolean next()
    {
        assertOpen();

        if ( iter.hasNext() )
        {
            current = iter.next();
            position += 1;
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public long skip( long elements )
    {
        if ( elements < 0 )
        {
            throw new IllegalArgumentException( "Cannot skip negative number of elements" );
        }
        else
        {
            int skipped = 0;
            while ( skipped < elements && next() )
            {
                skipped += 1;
            }
            return skipped;
        }
    }

    @Override
    public boolean first()
    {
        long pos = position();
        return pos < 0 ? next() : pos == 0;
    }

    @Override
    public boolean single()
    {
        return first() && atEnd();
    }

    @Override
    public List<Record> retain()
    {
        if ( isEmpty() )
        {
            return Collections.emptyList();
        }
        else if ( first() )
        {
            List<Record> result = new ArrayList<>();
            do
            {
                result.add( record() );
            }
            while ( next() );
            return result;
        }
        else
        {
            throw new
                    ClientException( String.format(
                    "Can't retain records when cursor is not pointing at the first record (currently at position %d)",
                    position ) );
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public ResultSummary summarize()
    {
        while ( next() ) ;
        return summary;
    }

    @Override
    public void close()
    {
        if ( open )
        {
            open = false;
        }
        else
        {
            throw new IllegalStateException( "Already closed" );
        }
    }

    private boolean isEmpty()
    {
        return position == -1 && !iter.hasNext();
    }

    private void assertOpen()
    {
        if ( !open )
        {
            throw new IllegalStateException( "Cursor already closed" );
        }
    }
}
