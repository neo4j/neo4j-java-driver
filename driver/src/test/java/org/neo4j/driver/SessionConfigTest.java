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

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.internal.Bookmark;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.SessionConfig.defaultConfig;
import static org.neo4j.driver.internal.InternalBookmark.parse;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

class SessionConfigTest
{
    @Test
    void shouldReturnDefaultValues() throws Throwable
    {
        SessionConfig config = defaultConfig();

        Assert.assertEquals( AccessMode.WRITE, config.defaultAccessMode() );
        assertFalse( config.database().isPresent() );
        assertNull( config.bookmarks() );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldChangeAccessMode( AccessMode mode ) throws Throwable
    {
        SessionConfig config = builder().withDefaultAccessMode( mode ).build();
        assertEquals( mode, config.defaultAccessMode() );
    }

    @ParameterizedTest
    @ValueSource( strings = {"foo", "data", "my awesome database", " "} )
    void shouldChangeDatabaseName( String databaseName )
    {
        SessionConfig config = builder().withDatabase( databaseName ).build();
        assertTrue( config.database().isPresent() );
        assertEquals( databaseName, config.database().get() );
    }

    @Test
    void shouldNotAllowNullDatabaseName() throws Throwable
    {
        assertThrows( NullPointerException.class, () -> builder().withDatabase( null ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"", ABSENT_DB_NAME} )
    void shouldForbiddenEmptyStringDatabaseName( String databaseName ) throws Throwable
    {
        IllegalArgumentException error = assertThrows( IllegalArgumentException.class, () -> builder().withDatabase( databaseName ) );
        assertThat( error.getMessage(), equalTo( "Illegal database name ''." ) );
    }

    @Test
    void shouldAcceptNullBookmarks() throws Throwable
    {
        SessionConfig config = builder().withBookmarks( (Bookmark[]) null ).build();
        assertNull( config.bookmarks() );

        SessionConfig config2 = builder().withBookmarks( (List<Bookmark>) null ).build();
        assertNull( config2.bookmarks() );
    }

    @Test
    void shouldAcceptEmptyBookmarks() throws Throwable
    {
        SessionConfig config = builder().withBookmarks().build();
        assertEquals( emptyList(), config.bookmarks() );

        SessionConfig config2 = builder().withBookmarks( emptyList() ).build();
        assertEquals( emptyList(), config2.bookmarks() );
    }

    @Test
    void shouldAcceptBookmarks() throws Throwable
    {
        Bookmark one = parse( "one" );
        Bookmark two = parse( "two" );
        SessionConfig config = builder().withBookmarks( one, two ).build();
        assertThat( config.bookmarks(), equalTo( Arrays.asList( one, two ) ) );

        SessionConfig config2 = builder().withBookmarks( Arrays.asList( one, two ) ).build();
        assertThat( config2.bookmarks(), equalTo( Arrays.asList( one, two ) ) );
    }

    @Test
    void shouldAcceptNullInBookmarks() throws Throwable
    {
        Bookmark one = parse( "one" );
        Bookmark two = parse( "two" );
        SessionConfig config = builder().withBookmarks( one, two, null ).build();
        assertThat( config.bookmarks(), equalTo( Arrays.asList( one, two, null ) ) );

        SessionConfig config2 = builder().withBookmarks( Arrays.asList( one, two, null ) ).build();
        assertThat( config2.bookmarks(), equalTo( Arrays.asList( one, two, null ) ) );
    }
}
