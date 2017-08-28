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
package org.neo4j.driver.internal.net;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.v1.Values.values;

public class SocketResponseHandlerTest
{
    private final SocketResponseHandler handler = new SocketResponseHandler();
    private final ResponseHandler responseHandler = mock( ResponseHandler.class );

    @Before
    public void setup()
    {
        handler.appendResponseHandler( responseHandler );
    }

    @Test
    public void shouldCollectRecords() throws Throwable
    {
        // Given
        Value[] record = values( 1, 2, 3 );

        // When
        handler.handleRecordMessage( record );

        // Then
        verify( responseHandler ).onRecord( record );
    }

    // todo: move these tests
//    @Test
//    public void shouldCollectFieldNames() throws Throwable
//    {
//        // Given
//        String[] fieldNames = new String[] { "name", "age", "income" };
//        Value fields = value( fieldNames );
//        Map<String, Value> data = parameters( "fields", fields ).asMap( ofValue());
//
//        // When
//        handler.handleSuccessMessage( data );
//
//        // Then
//        verify( responseHandler ).keys( fieldNames );
//    }
//
//    @Test
//    public void shouldCollectBasicMetadata() throws Throwable
//    {
//        // Given
//        Map<String, Value> data = parameters(
//                "type", "rw",
//                "stats", parameters(
//                        "nodes-created", 1,
//                        "properties-set", 12
//                )
//        ).asMap( ofValue());
//        SummaryCounters stats = new InternalSummaryCounters( 1, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0);
//
//        // When
//        handler.handleSuccessMessage( data );
//
//        // Then
//        verify( responseHandler ).statementType( StatementType.READ_WRITE );
//        verify( responseHandler ).statementStatistics( stats );
//    }
//
//    @Test
//    public void shouldCollectPlan() throws Throwable
//    {
//        // Given
//        Map<String, Value> data = parameters(
//            "type", "rw",
//            "stats", parameters(
//                    "nodes-created", 1,
//                    "properties-set", 12
//            ),
//            "plan", parameters(
//                        "operatorType", "ProduceResults",
//                        "identifiers", values( value( "num" ) ),
//                        "args", parameters( "KeyNames", "num", "EstimatedRows", 1.0 ),
//                        "children", values(
//                                parameters(
//                                    "operatorType", "Projection",
//                                    "identifiers", values( value( "num" ) ),
//                                    "args", parameters( "A", "x", "B", 2 ),
//                                    "children", emptyList()
//                                )
//                        )
//                )
//        ).asMap( ofValue());
//
//        SummaryCounters stats = new InternalSummaryCounters( 1, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0 );
//        Plan plan = plan(
//            "ProduceResults",
//                parameters( "KeyNames", "num", "EstimatedRows", 1.0 ).asMap( ofValue()), singletonList( "num" ),
//                singletonList(
//                plan( "Projection", parameters( "A", "x", "B", 2 ).asMap( ofValue()), singletonList( "num" ),
// Collections
//                        .<Plan>emptyList() )
//            )
//        );
//
//        // When
//        handler.handleSuccessMessage( data );
//
//        // Then
//        verify( responseHandler ).statementType( StatementType.READ_WRITE );
//        verify( responseHandler ).statementStatistics( stats );
//        verify( responseHandler ).plan( plan );
//    }
}
