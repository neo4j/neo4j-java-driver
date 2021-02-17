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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.QueryRunner;
import org.neo4j.driver.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public interface NestedQueries
{
    String OUTER_QUERY = "UNWIND range(1, 10000) AS x RETURN x";
    String INNER_QUERY = "UNWIND range(1, 10) AS y RETURN y";
    int EXPECTED_RECORDS = 10_000 * 10 + 10_000;

    Session newSession( AccessMode mode );

    @Test
    default void shouldAllowNestedQueriesInTransactionConsumedAsIterators() throws Exception
    {
        try ( Session session = newSession( AccessMode.READ ); Transaction tx = session.beginTransaction() )
        {
            testNestedQueriesConsumedAsIterators( tx );
            tx.commit();
        }
    }

    @Test
    default void shouldAllowNestedQueriesInTransactionConsumedAsLists() throws Exception
    {
        try ( Session session = newSession( AccessMode.READ ); Transaction tx = session.beginTransaction() )
        {
            testNestedQueriesConsumedAsLists( tx );
            tx.commit();
        }
    }

    @Test
    default void shouldAllowNestedQueriesInTransactionConsumedAsIteratorAndList() throws Exception
    {
        try ( Session session = newSession( AccessMode.READ ); Transaction tx = session.beginTransaction() )
        {
            testNestedQueriesConsumedAsIteratorAndList( tx );
            tx.commit();
        }
    }

    @Test
    default void shouldAllowNestedQueriesInSessionConsumedAsIterators() throws Exception
    {
        try ( Session session = newSession( AccessMode.READ ) )
        {
            testNestedQueriesConsumedAsIterators( session );
        }
    }

    @Test
    default void shouldAllowNestedQueriesInSessionConsumedAsLists() throws Exception
    {
        try ( Session session = newSession( AccessMode.READ ) )
        {
            testNestedQueriesConsumedAsLists( session );
        }
    }

    @Test
    default void shouldAllowNestedQueriesInSessionConsumedAsIteratorAndList() throws Exception
    {
        try ( Session session = newSession( AccessMode.READ ) )
        {
            testNestedQueriesConsumedAsIteratorAndList( session );
        }
    }

    default void testNestedQueriesConsumedAsIterators( QueryRunner queryRunner) throws Exception
    {
        int recordsSeen = 0;

        Result result1 = queryRunner.run( OUTER_QUERY );
        Thread.sleep( 1000 ); // allow some result records to arrive and be buffered

        while ( result1.hasNext() )
        {
            Record record1 = result1.next();
            assertFalse( record1.get( "x" ).isNull() );
            recordsSeen++;

            Result result2 = queryRunner.run( INNER_QUERY );
            while ( result2.hasNext() )
            {
                Record record2 = result2.next();
                assertFalse( record2.get( "y" ).isNull() );
                recordsSeen++;
            }
        }

        assertEquals( EXPECTED_RECORDS, recordsSeen );
    }

    default void testNestedQueriesConsumedAsLists( QueryRunner queryRunner) throws Exception
    {
        int recordsSeen = 0;

        Result result1 = queryRunner.run( OUTER_QUERY );
        Thread.sleep( 1000 ); // allow some result records to arrive and be buffered

        List<Record> records1 = result1.list();
        for ( Record record1 : records1 )
        {
            assertFalse( record1.get( "x" ).isNull() );
            recordsSeen++;

            Result result2 = queryRunner.run( "UNWIND range(1, 10) AS y RETURN y" );
            List<Record> records2 = result2.list();
            for ( Record record2 : records2 )
            {
                assertFalse( record2.get( "y" ).isNull() );
                recordsSeen++;
            }
        }

        assertEquals( EXPECTED_RECORDS, recordsSeen );
    }

    default void testNestedQueriesConsumedAsIteratorAndList( QueryRunner queryRunner) throws Exception
    {
        int recordsSeen = 0;

        Result result1 = queryRunner.run( OUTER_QUERY );
        Thread.sleep( 1000 ); // allow some result records to arrive and be buffered

        while ( result1.hasNext() )
        {
            Record record1 = result1.next();
            assertFalse( record1.get( "x" ).isNull() );
            recordsSeen++;

            Result result2 = queryRunner.run( "UNWIND range(1, 10) AS y RETURN y" );
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
