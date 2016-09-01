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
import java.util.Queue;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.summary.SummaryBuilder;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Functions;

import static java.util.Collections.emptyList;

public class InternalStatementResult implements StatementResult
{
    private final Connection connection;
    private final Collector runResponseCollector;
    private final Collector pullAllResponseCollector;
    private final Queue<Record> recordBuffer = new LinkedList<>();

    private List<String> keys = null;
    private ResultSummary summary = null;

    private long position = -1;
    private boolean done = false;

    InternalStatementResult( Connection connection, ExplicitTransaction transaction, Statement statement )
    {
        this.connection = connection;
        this.runResponseCollector = newRunResponseCollector();
        this.pullAllResponseCollector = newStreamResponseCollector( transaction, statement );
    }

    private Collector newRunResponseCollector()
    {
        return new Collector.NoOperationCollector()
        {
            @Override
            public void keys( String[] names )
            {
                keys = Arrays.asList( names );
            }

            @Override
            public void done()
            {
                if ( keys == null )
                {
                    keys = new ArrayList<>();
                }
            }

            @Override
            public void resultAvailableAfter( long l )
            {
              pullAllResponseCollector.resultAvailableAfter( l );
            }
        };
    }

    private Collector newStreamResponseCollector( final ExplicitTransaction transaction, final Statement statement )
    {
        final SummaryBuilder summaryBuilder = new SummaryBuilder( statement );

        return new Collector.NoOperationCollector()
        {
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
            public void statementStatistics( SummaryCounters statistics )
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
            public void bookmark( String bookmark )
            {
                if ( transaction != null )
                {
                    transaction.setBookmark( bookmark );
                }
            }

            @Override
            public void done()
            {
                summary = summaryBuilder.build();
                done = true;
            }

            @Override
            public void resultAvailableAfter(long l)
            {
                summaryBuilder.resultAvailableAfter( l );
            }

            @Override
            public void resultConsumedAfter(long l)
            {
                summaryBuilder.resultConsumedAfter( l );
            }
        };
    }

    Collector runResponseCollector()
    {
        return runResponseCollector;
    }

    Collector pullAllResponseCollector()
    {
        return pullAllResponseCollector;
    }

    @Override
    public List<String> keys()
    {
        if ( keys == null )
        {
            tryFetchNext();
        }
        return keys;
    }

    @Override
    public boolean hasNext()
    {
        return tryFetchNext();
    }

    @Override
    public Record next()
    {
        // Implementation note:
        // We've chosen to use Iterator<Record> over a cursor-based version,
        // after tests show escape analysis will eliminate short-lived allocations
        // in a way that makes the two equivalent in performance.
        // To get the intended benefit, we need to allocate Record in this method,
        // and have it copy out its fields from some lower level data structure.
        if ( tryFetchNext() )
        {
            position += 1;
            return recordBuffer.poll();
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
            return recordBuffer.peek();
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
        if ( done )
        {
            recordBuffer.clear();
        }
        else
        {
            do
            {
                connection.receiveOne();
                recordBuffer.clear();
            }
            while ( !done );
        }

        return summary;
    }

    @Override
    public void remove()
    {
        throw new ClientException( "Removing records from a result is not supported." );
    }

    private boolean tryFetchNext()
    {
        while ( recordBuffer.isEmpty() )
        {
            if ( done )
            {
                return false;
            }
            connection.receiveOne();
        }

        return true;
    }
}
