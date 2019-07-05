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
package org.neo4j.driver.internal;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.AccessMode;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.SessionConfig.defaultConfig;
import static org.neo4j.driver.internal.SessionConfig.builder;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

class SessionConfigTest
{
    @Test
    void shouldReturnDefaultValues() throws Throwable
    {
        SessionConfig parameters = defaultConfig();

        Assert.assertEquals( AccessMode.WRITE, parameters.defaultAccessMode() );
        assertFalse( parameters.database().isPresent() );
        assertNull( parameters.bookmarks() );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldChangeAccessMode( AccessMode mode ) throws Throwable
    {
        SessionConfig parameters = builder().withDefaultAccessMode( mode ).build();
        assertEquals( mode, parameters.defaultAccessMode() );
    }

    @ParameterizedTest
    @ValueSource( strings = {"foo", "data", "my awesome database", " "} )
    void shouldChangeDatabaseName( String databaseName )
    {
        SessionConfig parameters = builder().withDatabase( databaseName ).build();
        assertTrue( parameters.database().isPresent() );
        assertEquals( databaseName, parameters.database().get() );
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
        SessionConfig parameters = builder().withBookmarks( (String[]) null ).build();
        assertNull( parameters.bookmarks() );

        SessionConfig parameters2 = builder().withBookmarks( (List<String>) null ).build();
        assertNull( parameters2.bookmarks() );
    }

    @Test
    void shouldAcceptEmptyBookmarks() throws Throwable
    {
        SessionConfig parameters = builder().withBookmarks().build();
        assertEquals( emptyList(), parameters.bookmarks() );

        SessionConfig parameters2 = builder().withBookmarks( emptyList() ).build();
        assertEquals( emptyList(), parameters2.bookmarks() );
    }

    @Test
    void shouldAcceptBookmarks() throws Throwable
    {
        SessionConfig parameters = builder().withBookmarks( "one", "two" ).build();
        assertThat( parameters.bookmarks(), equalTo( Arrays.asList( "one", "two" ) ) );

        SessionConfig parameters2 = builder().withBookmarks( Arrays.asList( "one", "two" ) ).build();
        assertThat( parameters2.bookmarks(), equalTo( Arrays.asList( "one", "two" ) ) );
    }
}
