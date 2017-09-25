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
package org.neo4j.driver.internal.cluster;

import org.junit.Test;

import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RoutingProcedureResponseTest
{
    private static final Statement PROCEDURE = new Statement( "procedure" );

    private static final Record RECORD_1 = new InternalRecord( asList( "a", "b" ),
            new Value[]{new StringValue( "a" ), new StringValue( "b" )} );
    private static final Record RECORD_2 = new InternalRecord( asList( "a", "b" ),
            new Value[]{new StringValue( "aa" ), new StringValue( "bb" )} );

    @Test
    public void shouldBeSuccessfulWithRecords()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, asList( RECORD_1, RECORD_2 ) );
        assertTrue( response.isSuccess() );
    }

    @Test
    public void shouldNotBeSuccessfulWithError()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, new RuntimeException() );
        assertFalse( response.isSuccess() );
    }

    @Test
    public void shouldThrowWhenFailedAndAskedForRecords()
    {
        RuntimeException error = new RuntimeException();
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, error );

        try
        {
            response.records();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
            assertEquals( e.getCause(), error );
        }
    }

    @Test
    public void shouldThrowWhenSuccessfulAndAskedForError()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, asList( RECORD_1, RECORD_2 ) );

        try
        {
            response.error();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void shouldHaveErrorWhenFailed()
    {
        RuntimeException error = new RuntimeException( "Hi!" );
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, error );
        assertEquals( error, response.error() );
    }

    @Test
    public void shouldHaveRecordsWhenSuccessful()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, asList( RECORD_1, RECORD_2 ) );
        assertEquals( asList( RECORD_1, RECORD_2 ), response.records() );
    }

    @Test
    public void shouldHaveProcedure()
    {
        RoutingProcedureResponse response = new RoutingProcedureResponse( PROCEDURE, asList( RECORD_1, RECORD_2 ) );
        assertEquals( PROCEDURE, response.procedure() );
    }
}
