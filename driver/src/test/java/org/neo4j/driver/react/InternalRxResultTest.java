/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.react;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.internal.handlers.pulln.BasicPullResponseHandler;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.react.result.RxStatementResultCursor;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ResultSummary;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.v1.Values.values;

class InternalRxResultTest
{
    @Test
    void shouldObtainKeys() throws Throwable
    {
        // Given
        RxStatementResultCursor cursor = mock( RxStatementResultCursor.class );
        RxResult rxResult = newRxResult( cursor );

        List<String> keys = Arrays.asList( "one", "two", "three" );
        when( cursor.keys() ).thenReturn( Futures.completedWithValue( keys ) );

        // When
        List<String> allKeys = Flux.from( rxResult.keys() ).toStream().collect( Collectors.toList() );

        // Then
        assertThat( allKeys, equalTo( keys ) );
    }

    @Test
    void shouldErrorIfFailedToObtainKeys() throws Throwable
    {
        // Given
        RxStatementResultCursor cursor = mock( RxStatementResultCursor.class );
        RxResult rxResult = newRxResult( cursor );

        List<String> keys = Arrays.asList( "one", "two", "three" );
        when( cursor.keys() ).thenReturn( Futures.failedFuture( new RuntimeException( "Hi" ) ) );

        // When & Then
        assertThrows( RuntimeException.class, () -> Flux.from( rxResult.keys() ).toStream().collect( Collectors.toList() ) );
    }

    @Test
    void shouldObtainRecords() throws Throwable
    {
        // Given
        Record record1 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 1, 1, 1 ) );
        Record record2 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 2, 2, 2 ) );
        Record record3 = new InternalRecord( asList( "key1", "key2", "key3" ), values( 3, 3, 3 ) );

        BasicPullResponseHandler pullHandler = new ListBasedPullHandler( Arrays.asList( record1, record2, record3 ) );
        RxResult rxResult = newRxResult( pullHandler );
        Publisher<Record> publisher = rxResult.records();

        TestRecordingSubscriber<Record> subscriber = new TestRecordingSubscriber<>();
        publisher.subscribe( subscriber );

        // When
        List<Record> records = Flux.from( rxResult.records() ).toStream().collect( Collectors.toList() );

        // Then
        assertThat( records.size(), equalTo( 3 ) );
        assertThat( records.get( 0 ), equalTo( record1 ) );
        assertThat( records.get( 1 ), equalTo( record2 ) );
        assertThat( records.get( 2 ), equalTo( record3 ) );
    }

    @Test
    void shouldErrorIfFailedToStreamRecords() throws Throwable
    {
        // Given
        Throwable error = new RuntimeException( "Hi" );
        RxResult rxResult = newRxResult( new ListBasedPullHandler( null )
        {
            @Override
            public void request( long n )
            {
                summaryConsumer.accept( null, error );
                recordConsumer.accept( null, error );
            }
        } );

        // When & Then
        assertThrows( RuntimeException.class, () -> Flux.from( rxResult.records() ).toStream().collect( Collectors.toList() ) );
    }

    private InternalRxResult newRxResult( BasicPullResponseHandler pullHandler )
    {
        RunResponseHandler runHandler = mock( RunResponseHandler.class );
        when(runHandler.runFuture()).thenReturn( Futures.completedWithNull() );
        RxStatementResultCursor cursor = new RxStatementResultCursor( runHandler, pullHandler );
        return newRxResult( cursor );
    }

    private InternalRxResult newRxResult( RxStatementResultCursor cursor )
    {
        return new InternalRxResult( () -> {
            // for rx, the cursor will not wait for run result and will immediately return.
            return Futures.completedWithValue( cursor );
        } );
    }

    private static class ListBasedPullHandler implements BasicPullResponseHandler
    {
        private final List<Record> list;
        BiConsumer<Record,Throwable> recordConsumer;
        BiConsumer<ResultSummary,Throwable> summaryConsumer;
        private int index = 0;

        private ListBasedPullHandler( List<Record> list )
        {
            this.list = list;
        }

        @Override
        public void installRecordConsumer( BiConsumer<Record,Throwable> recordConsumer )
        {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void installSummaryConsumer( BiConsumer<ResultSummary,Throwable> summaryConsumer )
        {
            this.summaryConsumer = summaryConsumer;
        }

        @Override
        public boolean isFinishedOrCanceled()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isStreamingPaused()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onSuccess( Map<String,Value> metadata )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onFailure( Throwable error )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onRecord( Value[] fields )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void request( long n )
        {
            while ( index < list.size() && n-- > 0 )
            {
                this.recordConsumer.accept( list.get( index++ ), null );
            }

            if (index == list.size())
            {
                complete();
            }
        }

        @Override
        public void cancel()
        {
            complete();
        }

        private void complete()
        {
            this.recordConsumer.accept( null, null );
            this.summaryConsumer.accept( mock( ResultSummary.class ), null );
        }
    }
}
