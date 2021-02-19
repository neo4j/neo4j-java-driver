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
package org.neo4j.driver.internal.util;

import org.hamcrest.Matcher;

import java.util.HashSet;
import java.util.List;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.InternalBookmark;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.util.Iterables.asList;

public class BookmarkUtil
{
    /**
     * Bookmark is empty.
     */
    public static void assertBookmarkIsEmpty( Bookmark bookmark )
    {
        assertNotNull( bookmark );
        assertThat( bookmark, instanceOf( InternalBookmark.class ) );
        assertTrue( ((InternalBookmark) bookmark).isEmpty() );
    }

    /**
     * Bookmark is not empty but I do not care what value it has.
     */
    public static void assertBookmarkIsNotEmpty( Bookmark bookmark )
    {
        assertNotNull( bookmark );
        assertThat( bookmark, instanceOf( InternalBookmark.class ) );
        assertFalse( ((InternalBookmark) bookmark).isEmpty() );
    }

    /**
     * Bookmark contains one single value
     */
    public static void assertBookmarkContainsSingleValue( Bookmark bookmark )
    {
        assertBookmarkContainsSingleValue( bookmark, notNullValue( String.class ) );
    }

    /**
     * Bookmark contains one single value and the value matches the requirement set by matcher.
     */
    public static void assertBookmarkContainsSingleValue( Bookmark bookmark, Matcher<String> matcher )
    {
        assertNotNull( bookmark );
        assertThat( bookmark, instanceOf( InternalBookmark.class ) );

        List<String> values = asList( ((InternalBookmark) bookmark).values() );
        assertThat( values.size(), equalTo( 1 ) );
        assertThat( values.get( 0 ), matcher );
    }

    /**
     * Bookmark contains values matching the requirement set by matcher.
     */
    public static void assertBookmarkContainsValues( Bookmark bookmark, Matcher<Iterable<String>> matcher )
    {
        assertNotNull( bookmark );
        assertThat( bookmark, instanceOf( InternalBookmark.class ) );

        List<String> values = asList( ((InternalBookmark) bookmark).values() );
        assertThat( values, matcher );
    }

    /**
     * Each bookmark contains one single value and the values are all different from each other.
     */
    public static void assertBookmarksContainsSingleUniqueValues( Bookmark... bookmarks )
    {
        int count = bookmarks.length;
        HashSet<String> bookmarkStrings = new HashSet<>();

        for ( Bookmark bookmark : bookmarks )
        {
            assertBookmarkContainsSingleValue( bookmark );
            List<String> values = asList( ((InternalBookmark) bookmark).values() );
            bookmarkStrings.addAll( values );
        }
        assertEquals( count, bookmarkStrings.size() );
    }
}
