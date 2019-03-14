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
package org.neo4j.driver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.SessionParameters.builder;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

class SessionParametersTest
{
    @Test
    void shouldReturnDefaultValues() throws Throwable
    {
        SessionParameters empty = SessionParameters.empty();
        SessionParameters parameters = builder().build();

        assertEquals( empty, parameters );
        assertEquals( AccessMode.WRITE, empty.accessMode() );
        assertEquals( ABSENT_DB_NAME, empty.databaseName() );
        assertNull( empty.bookmark() );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldChangeAccessMode( AccessMode mode ) throws Throwable
    {
        SessionParameters parameters = builder().withAccessMode( mode ).build();
        assertEquals( mode, parameters.accessMode() );
    }

    @ParameterizedTest
    @ValueSource( strings = {"", "foo", "data", ABSENT_DB_NAME} )
    void shouldChangeDatabaseName( String databaseName )
    {
        SessionParameters parameters = builder().withDatabaseName( databaseName ).build();
        assertEquals( databaseName, parameters.databaseName() );
    }

    @Test
    void shouldForbiddenNullDatabaseName() throws Throwable
    {
        NullPointerException error = assertThrows( NullPointerException.class, () -> builder().withDatabaseName( null ) );
        assertThat( error.getMessage(), equalTo( "Database name cannot be null." ) );
    }

    @Test
    void shouldAcceptNullBookmarks() throws Throwable
    {
        SessionParameters parameters = builder().withBookmark( (String[]) null ).build();
        assertNull( parameters.bookmark() );

        SessionParameters parameters2 = builder().withBookmark( (List<String>) null ).build();
        assertNull( parameters2.bookmark() );
    }

    @Test
    void shouldAcceptEmptyBookmarks() throws Throwable
    {
        SessionParameters parameters = builder().withBookmark().build();
        assertEquals( emptyList(), parameters.bookmark() );

        SessionParameters parameters2 = builder().withBookmark( emptyList() ).build();
        assertEquals( emptyList(), parameters2.bookmark() );
    }

    @Test
    void shouldAcceptBookmarks() throws Throwable
    {
        SessionParameters parameters = builder().withBookmark( "one", "two" ).build();
        assertThat( parameters.bookmark(), equalTo( Arrays.asList( "one", "two" ) ) );

        SessionParameters parameters2 = builder().withBookmark( Arrays.asList( "one", "two" ) ).build();
        assertThat( parameters2.bookmark(), equalTo( Arrays.asList( "one", "two" ) ) );
    }
}
