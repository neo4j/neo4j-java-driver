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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

public class DelegatingTransactionContextTest
{
    Transaction transaction;
    DelegatingTransactionContext context;

    @BeforeEach
    void beforeEach()
    {
        transaction = mock( Transaction.class );
        context = new DelegatingTransactionContext( transaction );
    }

    @Test
    void shouldDelegateRunWithValueParams()
    {
        // GIVEN
        String query = "something";
        Value params = mock( Value.class );
        Result expected = mock( Result.class );
        given( transaction.run( query, params ) ).willReturn( expected );

        // WHEN
        Result actual = context.run( query, params );

        // THEN
        assertEquals( expected, actual );
        then( transaction ).should().run( query, params );
    }

    @Test
    void shouldDelegateRunWithMapParams()
    {
        // GIVEN
        String query = "something";
        Map<String,Object> params = Collections.emptyMap();
        Result expected = mock( Result.class );
        given( transaction.run( query, params ) ).willReturn( expected );

        // WHEN
        Result actual = context.run( query, params );

        // THEN
        assertEquals( expected, actual );
        then( transaction ).should().run( query, params );
    }

    @Test
    void shouldDelegateRunWithRecordParams()
    {
        // GIVEN
        String query = "something";
        Record params = mock( Record.class );
        Result expected = mock( Result.class );
        given( transaction.run( query, params ) ).willReturn( expected );

        // WHEN
        Result actual = context.run( query, params );

        // THEN
        assertEquals( expected, actual );
        then( transaction ).should().run( query, params );
    }

    @Test
    void shouldDelegateRun()
    {
        // GIVEN
        String query = "something";
        Result expected = mock( Result.class );
        given( transaction.run( query ) ).willReturn( expected );

        // WHEN
        Result actual = context.run( query );

        // THEN
        assertEquals( expected, actual );
        then( transaction ).should().run( query );
    }

    @Test
    void shouldDelegateRunWithQueryType()
    {
        // GIVEN
        Query query = mock( Query.class );
        Result expected = mock( Result.class );
        given( transaction.run( query ) ).willReturn( expected );

        // WHEN
        Result actual = context.run( query );

        // THEN
        assertEquals( expected, actual );
        then( transaction ).should().run( query );
    }
}
