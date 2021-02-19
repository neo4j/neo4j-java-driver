/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.reactive.util;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.Query;
import org.neo4j.driver.internal.handlers.PullResponseCompletionListener;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.internal.util.QueryKeys;
import org.neo4j.driver.internal.value.BooleanValue;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ResultSummary;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListBasedPullHandler extends BasicPullResponseHandler
{
    private final List<Record> list;
    private final Throwable error;
    private int index = 0;

    public ListBasedPullHandler()
    {
        this( emptyList(), null );
    }

    public ListBasedPullHandler( List<Record> list )
    {
        this( list, null );
    }

    public ListBasedPullHandler( Throwable error )
    {
        this( emptyList(), error );
    }

    private ListBasedPullHandler( List<Record> list, Throwable error )
    {
        super( mock( Query.class ), mock( RunResponseHandler.class ), mock( Connection.class ), mock( MetadataExtractor.class ), mock(
                PullResponseCompletionListener.class ) );
        this.list = list;
        this.error = error;
        when( super.metadataExtractor.extractSummary( any( Query.class ), any( Connection.class ), anyLong(), any( Map.class ) ) ).thenReturn(
                mock( ResultSummary.class ) );
        if ( list.size() > 1 )
        {
            Record record = list.get( 0 );
            when( super.runResponseHandler.queryKeys() ).thenReturn( new QueryKeys( record.keys() ) );
        }
    }

    @Override
    public void request( long n )
    {
        super.request( n );
        while ( index < list.size() && n-- > 0 )
        {
            onRecord( list.get( index++ ).values().toArray( new Value[0] ) );
        }

        if ( index == list.size() )
        {
            complete();
        }
        else
        {
            onSuccess( singletonMap( "has_more", BooleanValue.TRUE ) );
        }
    }

    @Override
    public void cancel()
    {
        super.cancel();
        complete();
    }

    private void complete()
    {
        if ( error != null )
        {
            onFailure( error );
        }
        else
        {
            onSuccess( emptyMap() );
        }
    }
}
