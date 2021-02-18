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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.async.ResultCursor;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.ParallelizableIT;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.util.TestUtil.await;

@ParallelizableIT
class QueryRunnerCloseIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private Driver driver;
    private ExecutorService executor;

    @AfterEach
    void tearDown()
    {
        if ( driver != null )
        {
            driver.close();
        }
        if ( executor != null )
        {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldErrorToAccessRecordsAfterConsume()
    {
        // Given
        Result result = neo4j.driver().session().run("UNWIND [1,2] AS a RETURN a");

        // When
        result.consume();

        // Then
        assertThrows( ResultConsumedException.class, result::hasNext );
        assertThrows( ResultConsumedException.class, result::next );
        assertThrows( ResultConsumedException.class, result::list );
        assertThrows( ResultConsumedException.class, result::single );
        assertThrows( ResultConsumedException.class, result::peek );
        assertThrows( ResultConsumedException.class, ()-> result.stream().toArray() );
        assertThrows( ResultConsumedException.class, () -> result.forEachRemaining( record -> {} ) );
        assertThrows( ResultConsumedException.class, () -> result.list( record -> record ) );
    }

    @Test
    void shouldErrorToAccessRecordsAfterClose()
    {
        // Given
        Session session = neo4j.driver().session();
        Result result = session.run("UNWIND [1,2] AS a RETURN a");

        // When
        session.close();

        // Then
        assertThrows( ResultConsumedException.class, result::hasNext );
        assertThrows( ResultConsumedException.class, result::next );
        assertThrows( ResultConsumedException.class, result::list );
        assertThrows( ResultConsumedException.class, result::single );
        assertThrows( ResultConsumedException.class, result::peek );
        assertThrows( ResultConsumedException.class, ()-> result.stream().toArray() );
        assertThrows( ResultConsumedException.class, () -> result.forEachRemaining( record -> {} ) );
        assertThrows( ResultConsumedException.class, () -> result.list( record -> record ) );
    }

    @Test
    void shouldAllowConsumeAndKeysAfterConsume()
    {
        // Given
        Result result = neo4j.driver().session().run("UNWIND [1,2] AS a RETURN a");
        List<String> keys = result.keys();

        // When
        ResultSummary summary = result.consume();

        // Then
        ResultSummary summary1 = result.consume();
        List<String> keys1 = result.keys();

        assertEquals( summary, summary1 );
        assertEquals( keys, keys1 );
    }

    @Test
    void shouldAllowSummaryAndKeysAfterClose()
    {
        // Given
        Session session = neo4j.driver().session();
        Result result = session.run("UNWIND [1,2] AS a RETURN a");
        List<String> keys = result.keys();
        ResultSummary summary = result.consume();

        // When
        session.close();

        // Then
        ResultSummary summary1 = result.consume();
        List<String> keys1 = result.keys();

        assertEquals( summary, summary1 );
        assertEquals( keys, keys1 );
    }

    @Test
    void shouldErrorToAccessRecordsAfterConsumeAsync()
    {
        // Given
        AsyncSession session = neo4j.driver().asyncSession();
        ResultCursor result = await( session.runAsync( "UNWIND [1,2] AS a RETURN a" ) );

        // When
        await( result.consumeAsync() );

        // Then
        assertThrows( ResultConsumedException.class, () -> await( result.nextAsync() ) );
        assertThrows( ResultConsumedException.class, () -> await( result.peekAsync() ) );
        assertThrows( ResultConsumedException.class, () -> await( result.singleAsync() ) );
        assertThrows( ResultConsumedException.class, () -> await( result.forEachAsync( record -> {} ) ) );
        assertThrows( ResultConsumedException.class, () -> await( result.listAsync() ) );
        assertThrows( ResultConsumedException.class, () -> await( result.listAsync( record -> record ) ) );
    }

    @Test
    void shouldErrorToAccessRecordsAfterCloseAsync()
    {
        // Given
        AsyncSession session = neo4j.driver().asyncSession();
        ResultCursor result = await( session.runAsync( "UNWIND [1,2] AS a RETURN a" ) );

        // When
        await( session.closeAsync() );

        // Then
        assertThrows( ResultConsumedException.class, () -> await( result.nextAsync() ) );
        assertThrows( ResultConsumedException.class, () -> await( result.peekAsync() ) );
        assertThrows( ResultConsumedException.class, () -> await( result.singleAsync() ) );
        assertThrows( ResultConsumedException.class, () -> await( result.forEachAsync( record -> {} ) ) );
        assertThrows( ResultConsumedException.class, () -> await( result.listAsync() ) );
        assertThrows( ResultConsumedException.class, () -> await( result.listAsync( record -> record ) ) );    }

    @Test
    void shouldAllowConsumeAndKeysAfterConsumeAsync()
    {
        // Given
        AsyncSession session = neo4j.driver().asyncSession();
        ResultCursor result = await( session.runAsync( "UNWIND [1,2] AS a RETURN a" ) );

        List<String> keys = result.keys();

        // When
        ResultSummary summary = await( result.consumeAsync() );

        // Then
        ResultSummary summary1 = await( result.consumeAsync() );
        List<String> keys1 = result.keys();

        assertEquals( summary, summary1 );
        assertEquals( keys, keys1 );
    }

    @Test
    void shouldAllowConsumeAndKeysAfterCloseAsync()
    {
        // Given
        AsyncSession session = neo4j.driver().asyncSession();
        ResultCursor result = await( session.runAsync( "UNWIND [1,2] AS a RETURN a" ) );
        List<String> keys = result.keys();
        ResultSummary summary = await( result.consumeAsync() );

        // When
        await( session.closeAsync() );

        // Then
        List<String> keys1 = result.keys();
        ResultSummary summary1 = await( result.consumeAsync() );

        assertEquals( summary, summary1 );
        assertEquals( keys, keys1 );
    }
}
