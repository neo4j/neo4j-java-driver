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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.StreamCollector;
import org.neo4j.driver.internal.summary.SummaryBuilder;
import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Notification;
import org.neo4j.driver.v1.Plan;
import org.neo4j.driver.v1.ProfiledPlan;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.RecordAccessor;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.ResultSummary;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementType;
import org.neo4j.driver.v1.UpdateStatistics;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.driver.v1.Records.recordAsIs;

public class InternalResultCursor extends InternalRecordAccessor implements ResultCursor
{
    private final Connection connection;
    private final StreamCollector runResponseCollector;
    private final StreamCollector pullAllResponseCollector;
    private final Queue<Record> recordBuffer = new LinkedList<>();

    private List<String> keys = null;
    private ResultSummary summary = null;

    private boolean open = true;
    private Record current = null;
    private long position = -1;
    private long limit = -1;
    private boolean done = false;

    public InternalResultCursor( Connection connection, String statement, Map<String,Value> parameters )
    {
        this.connection = connection;
        this.runResponseCollector = newRunResponseCollector();
        this.pullAllResponseCollector = newPullAllResponseCollector( new Statement( statement, parameters ) );
    }

    private StreamCollector newRunResponseCollector()
    {
        return new StreamCollector()
        {
            @Override
            public void keys( String[] names )
            {
                keys = Arrays.asList( names );
            }

            @Override
            public void record( Value[] fields ) {}

            @Override
            public void statementType( StatementType type ) {}

            @Override
            public void statementStatistics( UpdateStatistics statistics ) {}

            @Override
            public void plan( Plan plan ) {}

            @Override
            public void profile( ProfiledPlan plan ) {}

            @Override
            public void notifications( List<Notification> notifications ) {}

            @Override
            public void done()
            {
                if ( keys == null )
                {
                    keys = new ArrayList<>();
                }
            }
        };
    }

    private StreamCollector newPullAllResponseCollector( Statement statement )
    {
        final SummaryBuilder summaryBuilder = new SummaryBuilder( statement );
        return new StreamCollector()
        {
            @Override
            public void keys( String[] names ) {}

            @Override
            public void record( Value[] fields )
            {
                recordBuffer.add( new InternalRecord( keys, fields ) );
            }

            @Override
            public void statementType( StatementType type )
            {
                summaryBuilder.statementType( type );
            }

            @Override
            public void statementStatistics( UpdateStatistics statistics )
            {
                summaryBuilder.statementStatistics( statistics );
            }

            @Override
            public void plan( Plan plan )
            {
                summaryBuilder.plan( plan );
            }

            @Override
            public void profile( ProfiledPlan plan )
            {
                summaryBuilder.profile( plan );
            }

            @Override
            public void notifications( List<Notification> notifications )
            {
                summaryBuilder.notifications( notifications );
            }

            @Override
            public void done() {
                summary = summaryBuilder.build();
                done = true;
            }
        };
    }

    StreamCollector runResponseCollector()
    {
        return runResponseCollector;
    }

    StreamCollector pullAllResponseCollector()
    {
        return pullAllResponseCollector;
    }

    @Override
    public boolean isOpen()
    {
        return open;
    }

    public Value get( int index )
    {
        return record().get( index );
    }

    public Value get( String key )
    {
        return record().get( key );
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
        while (keys == null && !done)
        {
            connection.receiveOne();
        }
        return keys;
    }

    @Override
    public int size()
    {
        return keys.size();
    }

    @Override
    public Record record()
    {
        if ( current != null )
        {
            return current;
        }
        else
        {
            throw new NoSuchRecordException(
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
        if (!recordBuffer.isEmpty())
        {
            return false;
        }
        else if (done)
        {
            return true;
        }
        else
        {
            while ( recordBuffer.isEmpty() && !done )
            {
                connection.receiveOne();
            }
            return recordBuffer.isEmpty() && done;
        }
    }

    @Override
    public boolean next()
    {
        assertOpen();
        Record nextRecord = recordBuffer.poll();
        if ( nextRecord != null )
        {
            current = nextRecord;
            position += 1;
            if ( position == limit )
            {
                discard();
            }
            return true;
        }
        else if ( done )
        {
            return false;
        }
        else
        {
            while ( recordBuffer.isEmpty() && !done )
            {
                connection.receiveOne();
            }
            return next();
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
    public Record first()
    {
        if( position() >= 1 )
        {
            throw new NoSuchRecordException( "Cannot retrieve the first record, because this result cursor has been moved already. " +
                    "Please ensure you are not calling `first` multiple times, or are mixing it with calls " +
                    "to `next`, `single`, `list` or any other method that changes the position of the cursor." );
        }

        if( position == 0 )
        {
            return record();
        }

        if( !next() )
        {
            throw new NoSuchRecordException( "Cannot retrieve the first record, because this result is empty." );
        }
        return record();
    }


    @Override
    public Value first(String fieldName) throws NoSuchRecordException
    {
        return first().get( fieldName );
    }

    @Override
    public Value first(int index) throws NoSuchRecordException
    {
        return first().get( index );
    }

    @Override
    public Record single()
    {
        Record first = first();
        if( !atEnd() )
        {
            throw new NoSuchRecordException( "Expected a result with a single record, but this result contains at least one more. " +
                    "Ensure your query returns only one record, or use `first` instead of `single` if " +
                    "you do not care about the number of records in the result." );
        }
        return first;
    }

    @Override
    public Value single( String fieldName ) throws NoSuchRecordException
    {
        return single().get( fieldName );
    }

    @Override
    public Value single( int index ) throws NoSuchRecordException
    {
        return single().get( index );
    }

    @Override
    public Record peek()
    {
        assertOpen();
        Record nextRecord = recordBuffer.peek();
        if ( nextRecord != null )
        {
            return nextRecord;
        }
        else if ( done )
        {
            return null;
        }
        else
        {
            while ( recordBuffer.isEmpty() && !done )
            {
                connection.receiveOne();
            }
            return peek();
        }
    }

    @Override
    public List<Record> list()
    {
        return list( recordAsIs() );
    }

    @Override
    public <T> List<T> list( Function<RecordAccessor, T> mapFunction )
    {
        if ( isEmpty() )
        {
            assertOpen();
            return emptyList();
        }
        else if ( position == 0 || ( position == -1 && next() ) )
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
        return position == -1 && recordBuffer.isEmpty() && done;
    }

    private void discard()
    {
        assertOpen();
        recordBuffer.clear();
        while ( !done )
        {
            connection.receiveOne();
        }
    }

}
