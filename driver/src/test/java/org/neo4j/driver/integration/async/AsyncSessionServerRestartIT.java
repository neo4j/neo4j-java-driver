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
package org.neo4j.driver.integration.async;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import org.neo4j.driver.Record;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.AsyncTransaction;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.util.TestUtil.await;

@ParallelizableIT
class AsyncSessionServerRestartIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private AsyncSession session;

    @BeforeEach
    void setUp()
    {
        session = neo4j.driver().asyncSession();
    }

    @AfterEach
    void tearDown()
    {
        session.closeAsync();
    }

    @Test
    void shouldFailWhenServerIsRestarted()
    {
        int queryCount = 10_000;

        String query = "UNWIND range(1, 100) AS x " +
                       "CREATE (n1:Node {value: x})-[r:LINKED {value: x}]->(n2:Node {value: x}) " +
                       "DETACH DELETE n1, n2 " +
                       "RETURN x";

        assertThrows( ServiceUnavailableException.class, () ->
        {
            for ( int i = 0; i < queryCount; i++ )
            {
                ResultCursor cursor = await( session.runAsync( query ) );

                if ( i == 0 )
                {
                    neo4j.stopDb();
                }

                List<Record> records = await( cursor.listAsync() );
                assertEquals( 100, records.size() );
            }
        } );
        neo4j.startDb();
    }

    @Test
    void shouldRunAfterRunFailureToAcquireConnection()
    {
        neo4j.stopDb();

        assertThrows( ServiceUnavailableException.class, () ->
        {
            ResultCursor cursor = await( session.runAsync( "RETURN 42" ) );
            await( cursor.nextAsync() );
        } );

        neo4j.startDb();

        ResultCursor cursor2 = await( session.runAsync( "RETURN 42" ) );
        Record record = await( cursor2.singleAsync() );
        assertEquals( 42, record.get( 0 ).asInt() );
    }

    @Test
    void shouldBeginTxAfterRunFailureToAcquireConnection()
    {
        neo4j.stopDb();

        assertThrows( ServiceUnavailableException.class, () ->
        {
            ResultCursor cursor = await( session.runAsync( "RETURN 42" ) );
            await( cursor.consumeAsync() );
        } );

        neo4j.startDb();

        AsyncTransaction tx = await( session.beginTransactionAsync() );
        ResultCursor cursor2 = await( tx.runAsync( "RETURN 42" ) );
        Record record = await( cursor2.singleAsync() );
        assertEquals( 42, record.get( 0 ).asInt() );
        assertNull( await( tx.rollbackAsync() ) );
    }
}
