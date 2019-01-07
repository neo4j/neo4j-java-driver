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

class BookmarksTest
{
    @Test
    void isEmptyForEmptyBookmark()
    {
        Bookmarks bookmarks = Bookmarks.empty();
        assertTrue( bookmarks.isEmpty() );
    }

    @Test
    void maxBookmarkAsStringForEmptyBookmark()
    {
        Bookmarks bookmarks = Bookmarks.empty();
        assertNull( bookmarks.maxBookmarkAsString() );
    }

    @Test
    void asBeginTransactionParametersForEmptyBookmark()
    {
        Bookmarks bookmarks = Bookmarks.empty();
        assertEquals( emptyMap(), bookmarks.asBeginTransactionParameters() );
    }

    @Test
    void isEmptyForNonEmptyBookmark()
    {
        Bookmarks bookmarks = Bookmarks.from( "SomeBookmark" );
        assertFalse( bookmarks.isEmpty() );
    }

    @Test
    void maxBookmarkAsStringForNonEmptyBookmark()
    {
        Bookmarks bookmarks = Bookmarks.from( "SomeBookmark" );
        assertEquals( "SomeBookmark", bookmarks.maxBookmarkAsString() );
    }

    @Test
    void asBeginTransactionParametersForNonEmptyBookmark()
    {
        Bookmarks bookmarks = Bookmarks.from( "SomeBookmark" );
        verifyParameters( bookmarks, "SomeBookmark", "SomeBookmark" );
    }

    @Test
    void bookmarkFromString()
    {
        Bookmarks bookmarks = Bookmarks.from( "Cat" );
        assertEquals( "Cat", bookmarks.maxBookmarkAsString() );
        verifyParameters( bookmarks, "Cat", "Cat" );
    }

    @Test
    void bookmarkFromNullString()
    {
        Bookmarks bookmarks = Bookmarks.from( (String) null );
        assertTrue( bookmarks.isEmpty() );
    }

    @Test
    void bookmarkFromIterable()
    {
        Bookmarks bookmarks = Bookmarks.from( asList(
                "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" ) );
        assertEquals( "neo4j:bookmark:v1:tx42", bookmarks.maxBookmarkAsString() );
        verifyParameters( bookmarks,
                "neo4j:bookmark:v1:tx42",
                "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" );
    }

    @Test
    void bookmarkFromNullIterable()
    {
        Bookmarks bookmarks = Bookmarks.from( (Iterable<String>) null );
        assertTrue( bookmarks.isEmpty() );
    }

    @Test
    void bookmarkFromEmptyIterable()
    {
        Bookmarks bookmarks = Bookmarks.from( Collections.<String>emptyList() );
        assertTrue( bookmarks.isEmpty() );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithInvalidValue()
    {
        Bookmarks bookmarks = Bookmarks.from( asList(
                "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" ) );
        assertEquals( "neo4j:bookmark:v1:tx3", bookmarks.maxBookmarkAsString() );
        verifyParameters( bookmarks,
                "neo4j:bookmark:v1:tx3",
                "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithEmptyStringValue()
    {
        Bookmarks bookmarks = Bookmarks.from( asList( "neo4j:bookmark:v1:tx9", "", "neo4j:bookmark:v1:tx3" ) );
        assertEquals( "neo4j:bookmark:v1:tx9", bookmarks.maxBookmarkAsString() );
        verifyParameters( bookmarks,
                "neo4j:bookmark:v1:tx9",
                "neo4j:bookmark:v1:tx9", "", "neo4j:bookmark:v1:tx3" );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithNullValue()
    {
        Bookmarks bookmarks = Bookmarks.from( asList( "neo4j:bookmark:v1:tx41", null, "neo4j:bookmark:v1:tx42" ) );
        assertEquals( "neo4j:bookmark:v1:tx42", bookmarks.maxBookmarkAsString() );
        verifyParameters( bookmarks,
                "neo4j:bookmark:v1:tx42",
                asList( "neo4j:bookmark:v1:tx41", null, "neo4j:bookmark:v1:tx42" ) );
    }

    @Test
    void shouldReturnAllBookmarks()
    {
        assertIterableEquals( emptyList(), Bookmarks.empty().values() );
        assertIterableEquals( singletonList( "neo4j:bookmark:v1:tx42" ), Bookmarks.from( "neo4j:bookmark:v1:tx42" ).values() );

        List<String> bookmarks = asList( "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:tx2", "neo4j:bookmark:v1:tx3" );
        assertIterableEquals( bookmarks, Bookmarks.from( bookmarks ).values() );
    }

    private static void verifyParameters( Bookmarks bookmarks, String expectedMaxValue, String... expectedValues )
    {
        verifyParameters( bookmarks, expectedMaxValue, asList( expectedValues ) );
    }

    private static void verifyParameters( Bookmarks bookmarks, String expectedMaxValue, List<String> expectedValues )
    {
        Map<String,Value> expectedParameters = new HashMap<>();
        expectedParameters.put( "bookmark", value( expectedMaxValue ) );
        expectedParameters.put( "bookmarks", value( expectedValues ) );
        assertEquals( expectedParameters, bookmarks.asBeginTransactionParameters() );
    }
}
