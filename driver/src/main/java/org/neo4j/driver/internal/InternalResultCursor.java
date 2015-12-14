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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Pair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.RecordAccessor;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.ResultSummary;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoRecordException;

import static java.lang.String.format;
import static java.util.Collections.emptyList;

import static org.neo4j.driver.v1.Records.recordAsIs;

public class InternalResultCursor extends InternalRecordAccessor implements ResultCursor
{
    private final List<String> keys;
    private final PeekingIterator<Record> iter;
    private final ResultSummary summary;

    private boolean open = true;
    private Record current = null;
    private long position = -1;
    private long limit = -1;

    public InternalResultCursor( List<String> keys, List<Record> body, ResultSummary summary )
    {
        this.keys = keys;
        this.iter = new PeekingIterator<>( body.iterator() );
        this.summary = summary;
    }

    @Override
    public boolean isOpen()
    {
        return open;
    }

    public Value value( int index )
    {
        return record().value( index );
    }

    public Value value( String key )
    {
        return record().value( key );
    }

    @Override
    public boolean containsKey( String key )
    {
        return keys.contains( key );
    }

    @Override
    public int index( String key )
    {
        return record().index( key );
    }

    public List<String> keys()
    {
        return keys;
    }

    @Override
    public int size()
    {
        return keys.size();
    }

    @Override
    public boolean hasRecord()
    {
        assertOpen();
        return current != null && current.hasRecord();
    }

    @Override
    public Record record()
    {
        if ( hasRecord() )
        {
            return current;
        }
        else
        {
            throw new NoRecordException(
                "In order to access the fields of a record in a result, " +
                "you must first call next() to point the result to the next record in the result stream."
            );
        }
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
            if ( position == limit )
            {
                discard();
            }
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
            throw new ClientException( "Cannot skip negative number of elements" );
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
    public long limit( long records )
    {
        if ( records < 0 )
        {
            throw new ClientException( "Cannot limit negative number of elements" );
        }
        else if ( records == 0) {
            this.limit = position;
            discard();
        } else {
            this.limit = records + position;
        }
        return this.limit;
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
    public RecordAccessor peek()
    {
        return new PeekingRecordAccessor();
    }

    @Override
    public List<Record> retain()
    {
        return retain( recordAsIs() );
    }

    @Override
    public <T> List<T> retain( Function<RecordAccessor, T> mapFunction )
    {
        if ( isEmpty() )
        {
            assertOpen();
            return emptyList();
        }
        else if ( first() )
        {
            List<T> result = new ArrayList<>();
            do
            {
                result.add( mapFunction.apply( this ) );
            }
            while ( next() );
            discard();
            return result;
        }
        else
        {
            throw new ClientException(
                format( "Can't retain records when cursor is not pointing at the first record (currently at position %d)", position )
            );
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public ResultSummary summarize()
    {
        while ( next() ) ;
        discard();
        return summary;
    }

    @Override
    public void close()
    {
        if ( open )
        {
            discard();
            open = false;
        }
        else
        {
            throw new ClientException( "Already closed" );
        }
    }

    private void assertOpen()
    {
        if ( !open )
        {
            throw new ClientException( "Cursor already closed" );
        }
    }

    private boolean isEmpty()
    {
        return position == -1 && !iter.hasNext();
    }

    private void discard()
    {
        iter.discard();
    }

    private class PeekingRecordAccessor implements RecordAccessor
    {
        @Override
        public List<String> keys()
        {
            return InternalResultCursor.this.keys();
        }

        @Override
        public boolean containsKey( String key )
        {
            return InternalResultCursor.this.containsKey( key );
        }

        @Override
        public int index( String key )
        {
            return InternalResultCursor.this.index( key );
        }

        @Override
        public Value value( String key )
        {
            return record().value( key );
        }

        @Override
        public int size()
        {
            return InternalResultCursor.this.size();
        }

        @Override
        public List<Pair<String, Value>> fields()
        {
            return record().fields();
        }

        @Override
        public boolean hasRecord()
        {
            return iter.hasNext();
        }

        @Override
        public Record record()
        {
            Record record = iter.peek();
            if ( record == null )
            {
                throw new NoRecordException( "Cannot peek past last record" );
            }
            else
            {
                return record;
            }
        }

        @Override
        public Value value( int index )
        {
            return record().value( index );
        }
    }
}
