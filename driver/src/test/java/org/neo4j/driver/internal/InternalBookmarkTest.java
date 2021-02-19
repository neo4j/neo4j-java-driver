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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;

import org.neo4j.driver.Bookmark;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.InternalBookmark.parse;
import static org.neo4j.driver.util.TestUtil.asSet;

class InternalBookmarkTest
{
    @Test
    void isEmptyForEmptyBookmark()
    {
        Bookmark bookmark = InternalBookmark.empty();
        assertTrue( bookmark.isEmpty() );
        assertEquals( emptySet(), bookmark.values() );
    }

    @Test
    void shouldSetToEmptyForNullBookmark() throws Throwable
    {
        Bookmark bookmark = InternalBookmark.from( null );
        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldSetToEmptyForEmptyBookmarkIterator() throws Throwable
    {
        Bookmark bookmark = InternalBookmark.from( emptyList() );
        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldSetToEmptyForNullBookmarkList() throws Throwable
    {
        Bookmark bookmark = InternalBookmark.from( singletonList( null ) );
        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldIgnoreNullAndEmptyInBookmarkList() throws Throwable
    {
        Bookmark bookmark = InternalBookmark.from( Arrays.asList( InternalBookmark.empty(), null, null ) );
        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldReserveBookmarkValuesCorrectly() throws Throwable
    {
        Bookmark one = parse( "one" );
        Bookmark two = parse( "two" );
        Bookmark empty = InternalBookmark.empty();
        Bookmark bookmark = InternalBookmark.from( Arrays.asList( one, two, null, empty ) );
        verifyValues( bookmark, "one", "two" );
    }

    @Test
    void isNotEmptyForNonEmptyBookmark()
    {
        Bookmark bookmark = InternalBookmark.parse( "SomeBookmark" );
        assertFalse( bookmark.isEmpty() );
    }

    @Test
    void asBeginTransactionParametersForNonEmptyBookmark()
    {
        Bookmark bookmark = InternalBookmark.parse( "SomeBookmark" );
        verifyValues( bookmark, "SomeBookmark" );
    }

    @Test
    void bookmarkFromString()
    {
        Bookmark bookmark = InternalBookmark.parse( "Cat" );
        assertEquals( singleton( "Cat" ), bookmark.values() );
        verifyValues( bookmark, "Cat" );
    }

    @Test
    void bookmarkFromNullString()
    {
        Bookmark bookmark = InternalBookmark.parse( (String) null );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void bookmarkFromSet()
    {
        Set<String> input = asSet( "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" );
        Bookmark bookmark = InternalBookmark.parse( input );
        verifyValues( bookmark, "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" );
    }

    @Test
    void bookmarkFromNullIterable()
    {
        Bookmark bookmark = InternalBookmark.parse( (Set<String>) null );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void bookmarkFromEmptyIterable()
    {
        Bookmark bookmark = InternalBookmark.parse( emptySet() );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithInvalidValue()
    {
        Bookmark bookmark = InternalBookmark.parse( asSet( "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" ) );
        verifyValues( bookmark, "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" );
    }

    @Test
    void shouldReturnAllBookmarks()
    {
        assertIterableEquals( emptyList(), InternalBookmark.empty().values() );
        assertIterableEquals( singleton( "neo4j:bookmark:v1:tx42" ), InternalBookmark.parse( "neo4j:bookmark:v1:tx42" ).values() );

        Set<String> bookmarks = asSet( "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:tx2", "neo4j:bookmark:v1:tx3" );
        assertIterableEquals( bookmarks, InternalBookmark.parse( bookmarks ).values() );
    }

    @Test
    void valueShouldBeReadOnly() throws Throwable
    {
        Bookmark bookmark = InternalBookmark.parse( asSet( "first", "second" ) );
        Set<String> values = bookmark.values();
        assertThrows( UnsupportedOperationException.class, () -> values.add( "third" ) );
    }

    private static void verifyValues( Bookmark bookmark, String... expectedValues )
    {
        assertThat( bookmark.values().size(), equalTo( expectedValues.length ) );
        assertThat( bookmark.values(), hasItems( expectedValues ) );
    }
}
