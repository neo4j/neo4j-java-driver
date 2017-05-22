/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.Value;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.Values.value;

public class BookmarkTest
{
    @Test
    public void isEmptyForEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.empty();
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    public void asStringForEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.empty();
        assertNull( bookmark.asString() );
    }

    @Test
    public void asParametersForEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.empty();
        assertEquals( emptyMap(), bookmark.asParameters() );
    }

    @Test
    public void isEmptyForNonEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.from( "SomeBookmark" );
        assertFalse( bookmark.isEmpty() );
    }

    @Test
    public void asStringForNonEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.from( "SomeBookmark" );
        assertEquals( "SomeBookmark", bookmark.asString() );
    }

    @Test
    public void asParametersForNonEmptyBookmark()
    {
        Bookmark bookmark = Bookmark.from( "SomeBookmark" );
        verifyParameters( bookmark, "SomeBookmark", "SomeBookmark" );
    }

    @Test
    public void bookmarkFromString()
    {
        Bookmark bookmark = Bookmark.from( "Cat" );
        assertEquals( "Cat", bookmark.asString() );
        verifyParameters( bookmark, "Cat", "Cat" );
    }

    @Test
    public void bookmarkFromNullString()
    {
        Bookmark bookmark = Bookmark.from( (String) null );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    public void bookmarkFromIterable()
    {
        Bookmark bookmark = Bookmark.from( asList(
                "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" ) );
        assertEquals( "neo4j:bookmark:v1:tx42", bookmark.asString() );
        verifyParameters( bookmark,
                "neo4j:bookmark:v1:tx42",
                "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" );
    }

    @Test
    public void bookmarkFromNullIterable()
    {
        Bookmark bookmark = Bookmark.from( (Iterable<String>) null );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    public void bookmarkFromEmptyIterable()
    {
        Bookmark bookmark = Bookmark.from( Collections.<String>emptyList() );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    public void asParametersForBookmarkWithInvalidValue()
    {
        Bookmark bookmark = Bookmark.from( asList(
                "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" ) );
        assertEquals( "neo4j:bookmark:v1:tx3", bookmark.asString() );
        verifyParameters( bookmark,
                "neo4j:bookmark:v1:tx3",
                "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" );
    }

    private static void verifyParameters( Bookmark bookmark, String bookmarkValue, String... bookmarkValues )
    {
        Map<String,Value> expectedParameters = new HashMap<>();
        expectedParameters.put( "bookmark", value( bookmarkValue ) );
        expectedParameters.put( "bookmarks", value( bookmarkValues ) );
        assertEquals( expectedParameters, bookmark.asParameters() );
    }
}
