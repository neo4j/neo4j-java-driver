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
package org.neo4j.driver.internal.cluster;

import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.Record;
import org.neo4j.driver.Query;
import org.neo4j.driver.Value;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingProcedureResponseTest
{
    private static final Query PROCEDURE = new Query( "procedure" );

    private static final Record RECORD_1 = new InternalRecord( asList( "a", "b" ),
            new Value[]{new StringValue( "a" ), new StringValue( "b" )} );
    private static final Record RECORD_2 = new InternalRecord( asList( "a", "b" ),
            new Value[]{new StringValue( "aa" ), new StringValue( "bb" )} );

    @Test
    void shouldBeSuccessfulWithRecords()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, asList( RECORD_1, RECORD_2 ) );
        assertTrue( response.isSuccess() );
    }

    @Test
    void shouldNotBeSuccessfulWithError()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, new RuntimeException() );
        assertFalse( response.isSuccess() );
    }

    @Test
    void shouldThrowWhenFailedAndAskedForRecords()
    {
        RuntimeException error = new RuntimeException();
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, error );

        IllegalStateException e = assertThrows( IllegalStateException.class, response::records );
        assertEquals( e.getCause(), error );
    }

    @Test
    void shouldThrowWhenSuccessfulAndAskedForError()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, asList( RECORD_1, RECORD_2 ) );

        assertThrows( IllegalStateException.class, response::error );
    }

    @Test
    void shouldHaveErrorWhenFailed()
    {
        RuntimeException error = new RuntimeException( "Hi!" );
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, error );
        assertEquals( error, response.error() );
    }

    @Test
    void shouldHaveRecordsWhenSuccessful()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, asList( RECORD_1, RECORD_2 ) );
        assertEquals( asList( RECORD_1, RECORD_2 ), response.records() );
    }

    @Test
    void shouldHaveProcedure()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, asList( RECORD_1, RECORD_2 ) );
        assertEquals( PROCEDURE, response.procedure() );
    }
}
