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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

class SessionParametersTest
{
    @Test
    void shouldReturnDefaultValues() throws Throwable
    {
        SessionParameters parameters = new SessionParameters();

        assertEquals( AccessMode.WRITE, parameters.defaultAccessMode() );
        assertEquals( ABSENT_DB_NAME, parameters.database() );
        assertNull( parameters.bookmarks() );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldChangeAccessMode( AccessMode mode ) throws Throwable
    {
        SessionParameters parameters = new SessionParameters().withDefaultAccessMode( mode );
        assertEquals( mode, parameters.defaultAccessMode() );
    }

    @ParameterizedTest
    @ValueSource( strings = {"", "foo", "data", ABSENT_DB_NAME} )
    void shouldChangeDatabaseName( String databaseName )
    {
        SessionParameters parameters = new SessionParameters().withDatabase( databaseName );
        assertEquals( databaseName, parameters.database() );
    }

    @Test
    void shouldForbiddenNullDatabaseName() throws Throwable
    {
        NullPointerException error = assertThrows( NullPointerException.class, () -> new SessionParameters().withDatabase( null ) );
        assertThat( error.getMessage(), equalTo( "Database cannot be null." ) );
    }

    @Test
    void shouldAcceptNullBookmarks() throws Throwable
    {
        SessionParameters parameters = new SessionParameters().withBookmarks( (String[]) null );
        assertNull( parameters.bookmarks() );

        SessionParameters parameters2 = new SessionParameters().withBookmarks( (List<String>) null );
        assertNull( parameters2.bookmarks() );
    }

    @Test
    void shouldAcceptEmptyBookmarks() throws Throwable
    {
        SessionParameters parameters = new SessionParameters().withBookmarks();
        assertEquals( emptyList(), parameters.bookmarks() );

        SessionParameters parameters2 = new SessionParameters().withBookmarks( emptyList() );
        assertEquals( emptyList(), parameters2.bookmarks() );
    }

    @Test
    void shouldAcceptBookmarks() throws Throwable
    {
        SessionParameters parameters = new SessionParameters().withBookmarks( "one", "two" );
        assertThat( parameters.bookmarks(), equalTo( Arrays.asList( "one", "two" ) ) );

        SessionParameters parameters2 = new SessionParameters().withBookmarks( Arrays.asList( "one", "two" ) );
        assertThat( parameters2.bookmarks(), equalTo( Arrays.asList( "one", "two" ) ) );
    }

    @Test
    void shouldDeepClone() throws Throwable
    {
        // Given
        List<String> bookmarks = Arrays.asList( "one", "two" );
        AccessMode mode = AccessMode.READ;
        String databaseName = "foo";
        SessionParameters parameters = new SessionParameters().withBookmarks( bookmarks ).withDefaultAccessMode( mode ).withDatabase( databaseName );

        SessionParameters clone = parameters.clone();

        assertNotSame( clone, parameters );
        assertNotSame( clone.bookmarks(), bookmarks );

        assertSame( clone.database(), databaseName );
        assertSame( clone.defaultAccessMode(), mode );

        assertEquals( parameters, clone );
    }

    @Test
    void canCloneNullBookmarks() throws Throwable
    {
        SessionParameters parameters = new SessionParameters();
        SessionParameters clone = parameters.clone();

        assertEquals( parameters, clone );
        assertEquals( AccessMode.WRITE, parameters.defaultAccessMode() );
        assertEquals( ABSENT_DB_NAME, parameters.database() );
        assertNull( parameters.bookmarks() );
    }
}
