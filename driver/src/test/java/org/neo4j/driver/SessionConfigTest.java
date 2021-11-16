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
package org.neo4j.driver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.driver.SessionConfig.builder;
import static org.neo4j.driver.SessionConfig.defaultConfig;
import static org.neo4j.driver.internal.InternalBookmark.parse;

class SessionConfigTest
{
    @Test
    void shouldReturnDefaultValues() throws Throwable
    {
        SessionConfig config = defaultConfig();

        assertEquals( AccessMode.WRITE, config.defaultAccessMode() );
        assertFalse( config.database().isPresent() );
        assertNull( config.bookmarks() );
        assertFalse( config.fetchSize().isPresent() );
    }

    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void shouldChangeAccessMode( AccessMode mode ) throws Throwable
    {
        SessionConfig config = builder().withDefaultAccessMode( mode ).build();
        assertEquals( mode, config.defaultAccessMode() );
    }

    @ParameterizedTest
    @ValueSource( strings = {"foo", "data", "my awesome database", "    "} )
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
    @MethodSource("someConfigs")
    void nullDatabaseNameMustNotBreakEquals(SessionConfig config1, SessionConfig config2, boolean expectedEquals) {

        assertEquals( config1.equals( config2 ), expectedEquals );
    }

    static Stream<Arguments> someConfigs() {
        return Stream.of(
                arguments( SessionConfig.builder().build(), SessionConfig.builder().build(), true ),
                arguments( SessionConfig.builder().withDatabase( "a" ).build(), SessionConfig.builder().build(), false ),
                arguments( SessionConfig.builder().build(), SessionConfig.builder().withDatabase( "a" ).build(), false ),
                arguments( SessionConfig.builder().withDatabase( "a" ).build(), SessionConfig.builder().withDatabase( "a" ).build(), true )
        );
    }

    @ParameterizedTest
    @ValueSource( strings = {""} )
    void shouldForbiddenEmptyStringDatabaseName( String databaseName ) throws Throwable
    {
        IllegalArgumentException error = assertThrows( IllegalArgumentException.class, () -> builder().withDatabase( databaseName ) );
        assertThat( error.getMessage(), startsWith( "Illegal database name " ) );
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

    @ParameterizedTest
    @ValueSource( longs = {100, 1, 1000, Long.MAX_VALUE, -1} )
    void shouldChangeFetchSize( long value ) throws Throwable
    {
        SessionConfig config = builder().withFetchSize( value ).build();
        assertThat( config.fetchSize(), equalTo( Optional.of( value ) ) );
    }

    @ParameterizedTest
    @ValueSource( longs = {0, -100, -2} )
    void shouldErrorWithIllegalFetchSize( long value ) throws Throwable
    {
        assertThrows( IllegalArgumentException.class, () -> builder().withFetchSize( value ).build() );
    }

    @Test
    void shouldTwoConfigBeEqual() throws Throwable
    {
        SessionConfig config1 = builder().withFetchSize( 100 ).build();
        SessionConfig config2 = builder().withFetchSize( 100 ).build();

        assertEquals( config1, config2 );
    }
}
