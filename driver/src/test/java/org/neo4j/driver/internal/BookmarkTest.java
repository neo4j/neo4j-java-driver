/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Value;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.v1.Values.value;

class BookmarkTest
{
    @Test
    void isEmptyForEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.empty();
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void maxBookmarkAsStringForEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.empty();
        assertNull( bookmark.maxBookmarkAsString() );
    }

    @Test
    void asBeginTransactionParametersForEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.empty();
        assertEquals( emptyMap(), bookmark.asBeginTransactionParameters() );
    }

    @Test
    void isEmptyForNonEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.from( "SomeBookmark" );
        assertFalse( bookmark.isEmpty() );
    }

    @Test
    void maxBookmarkAsStringForNonEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.from( "SomeBookmark" );
        assertEquals( "SomeBookmark", bookmark.maxBookmarkAsString() );
    }

    @Test
    void asBeginTransactionParametersForNonEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.from( "SomeBookmark" );
        verifyParameters( bookmark, "SomeBookmark", "SomeBookmark" );
    }

    @Test
    void bookmarkFromString()
    {
        Bookmark bookmark = Bookmark.from( "Cat" );
        assertEquals( "Cat", bookmark.maxBookmarkAsString() );
        verifyParameters( bookmark, "Cat", "Cat" );
    }

    @Test
    void bookmarkFromNullString()
    {
        Bookmark bookmark = Bookmark.from( (String) null );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void bookmarkFromIterable()
    {
        Bookmark bookmark = Bookmark.from( asList(
                "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" ) );
        assertEquals( "neo4j:bookmark:v1:tx42", bookmark.maxBookmarkAsString() );
        verifyParameters( bookmark,
                "neo4j:bookmark:v1:tx42",
                "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" );
    }

    @Test
    void bookmarkFromNullIterable()
    {
        Bookmark bookmark = Bookmark.from( (Iterable<String>) null );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void bookmarkFromEmptyIterable()
    {
        Bookmark bookmark = Bookmark.from( Collections.<String>emptyList() );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithInvalidValue()
    {
        Bookmark bookmark = Bookmark.from( asList(
                "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" ) );
        assertEquals( "neo4j:bookmark:v1:tx3", bookmark.maxBookmarkAsString() );
        verifyParameters( bookmark,
                "neo4j:bookmark:v1:tx3",
                "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithEmptyStringValue()
    {
        Bookmark bookmark = Bookmark.from( asList( "neo4j:bookmark:v1:tx9", "", "neo4j:bookmark:v1:tx3" ) );
        assertEquals( "neo4j:bookmark:v1:tx9", bookmark.maxBookmarkAsString() );
        verifyParameters( bookmark,
                "neo4j:bookmark:v1:tx9",
                "neo4j:bookmark:v1:tx9", "", "neo4j:bookmark:v1:tx3" );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithNullValue()
    {
        Bookmark bookmark = Bookmark.from( asList( "neo4j:bookmark:v1:tx41", null, "neo4j:bookmark:v1:tx42" ) );
        assertEquals( "neo4j:bookmark:v1:tx42", bookmark.maxBookmarkAsString() );
        verifyParameters( bookmark,
                "neo4j:bookmark:v1:tx42",
                asList( "neo4j:bookmark:v1:tx41", null, "neo4j:bookmark:v1:tx42" ) );
    }

    @Test
    void shouldReturnAllBookmarks()
    {
        assertIterableEquals( emptyList(), Bookmark.empty().values() );
        assertIterableEquals( singletonList( "neo4j:bookmark:v1:tx42" ), Bookmark.from( "neo4j:bookmark:v1:tx42" ).values() );

        List<String> bookmarks = asList( "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:tx2", "neo4j:bookmark:v1:tx3" );
        assertIterableEquals( bookmarks, Bookmark.from( bookmarks ).values() );
    }

    private static void verifyParameters( Bookmark bookmark, String expectedMaxValue, String... expectedValues )
    {
        verifyParameters( bookmark, expectedMaxValue, asList( expectedValues ) );
    }

    private static void verifyParameters( Bookmark bookmark, String expectedMaxValue, List<String> expectedValues )
    {
        Map<String,Value> expectedParameters = new HashMap<>();
        expectedParameters.put( "bookmark", value( expectedMaxValue ) );
        expectedParameters.put( "bookmarks", value( expectedValues ) );
        assertEquals( expectedParameters, bookmark.asBeginTransactionParameters() );
    }
}
