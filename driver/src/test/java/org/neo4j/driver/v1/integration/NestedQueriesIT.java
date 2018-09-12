/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementRunner;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.util.SessionExtension;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class NestedQueriesIT
{
    private static final String OUTER_QUERY = "UNWIND range(1, 10000) AS x RETURN x";
    private static final String INNER_QUERY = "UNWIND range(1, 10) AS y RETURN y";
    private static final int EXPECTED_RECORDS = 10_000 * 10 + 10_000;
    private static final Duration TIMEOUT = Duration.ofMinutes( 2 );

    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldAllowNestedQueriesInTransactionConsumedAsIterators()
    {
        assertTimeoutPreemptively( TIMEOUT, () ->
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                testNestedQueriesConsumedAsIterators( tx );
                tx.success();
            }
        } );
    }

    @Test
    void shouldAllowNestedQueriesInTransactionConsumedAsLists()
    {
        assertTimeoutPreemptively( TIMEOUT, () ->
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                testNestedQueriesConsumedAsLists( tx );
                tx.success();
            }
        } );
    }

    @Test
    void shouldAllowNestedQueriesInTransactionConsumedAsIteratorAndList()
    {
        assertTimeoutPreemptively( TIMEOUT, () ->
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                testNestedQueriesConsumedAsIteratorAndList( tx );
                tx.success();
            }
        } );
    }

    @Test
    void shouldAllowNestedQueriesInSessionConsumedAsIterators()
    {
        assertTimeoutPreemptively( TIMEOUT, () -> testNestedQueriesConsumedAsIterators( session ) );
    }

    @Test
    void shouldAllowNestedQueriesInSessionConsumedAsLists()
    {
        assertTimeoutPreemptively( TIMEOUT, () -> testNestedQueriesConsumedAsLists( session ) );
    }

    @Test
    void shouldAllowNestedQueriesInSessionConsumedAsIteratorAndList()
    {
        assertTimeoutPreemptively( TIMEOUT, () -> testNestedQueriesConsumedAsIteratorAndList( session ) );
    }

    private void testNestedQueriesConsumedAsIterators( StatementRunner statementRunner ) throws Exception
    {
        int recordsSeen = 0;

        StatementResult result1 = statementRunner.run( OUTER_QUERY );
        Thread.sleep( 1000 ); // allow some result records to arrive and be buffered

        while ( result1.hasNext() )
        {
            Record record1 = result1.next();
            assertFalse( record1.get( "x" ).isNull() );
            recordsSeen++;

            StatementResult result2 = statementRunner.run( INNER_QUERY );
            while ( result2.hasNext() )
            {
                Record record2 = result2.next();
                assertFalse( record2.get( "y" ).isNull() );
                recordsSeen++;
            }
        }

        assertEquals( EXPECTED_RECORDS, recordsSeen );
    }

    private void testNestedQueriesConsumedAsLists( StatementRunner statementRunner ) throws Exception
    {
        int recordsSeen = 0;

        StatementResult result1 = statementRunner.run( OUTER_QUERY );
        Thread.sleep( 1000 ); // allow some result records to arrive and be buffered

        List<Record> records1 = result1.list();
        for ( Record record1 : records1 )
        {
            assertFalse( record1.get( "x" ).isNull() );
            recordsSeen++;

            StatementResult result2 = statementRunner.run( "UNWIND range(1, 10) AS y RETURN y" );
            List<Record> records2 = result2.list();
            for ( Record record2 : records2 )
            {
                assertFalse( record2.get( "y" ).isNull() );
                recordsSeen++;
            }
        }

        assertEquals( EXPECTED_RECORDS, recordsSeen );
    }

    private void testNestedQueriesConsumedAsIteratorAndList( StatementRunner statementRunner ) throws Exception
    {
        int recordsSeen = 0;

        StatementResult result1 = statementRunner.run( OUTER_QUERY );
        Thread.sleep( 1000 ); // allow some result records to arrive and be buffered

        while ( result1.hasNext() )
        {
            Record record1 = result1.next();
            assertFalse( record1.get( "x" ).isNull() );
            recordsSeen++;

            StatementResult result2 = statementRunner.run( "UNWIND range(1, 10) AS y RETURN y" );
            List<Record> records2 = result2.list();
            for ( Record record2 : records2 )
            {
                assertFalse( record2.get( "y" ).isNull() );
                recordsSeen++;
            }
        }

        assertEquals( EXPECTED_RECORDS, recordsSeen );
    }
}
