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

import java.util.Collections;
import java.util.Set;

import org.neo4j.driver.Bookmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultBookmarkHolderTest
{
    @Test
    void shouldAllowToGetAndSetBookmarks()
    {
        BookmarksHolder bookmarkHolder = new DefaultBookmarksHolder();
        assertTrue( bookmarkHolder.getBookmarks().isEmpty() );

        bookmarkHolder.setBookmark( null );
        assertTrue( bookmarkHolder.getBookmarks().isEmpty() );

        Bookmark bookmark1 = InternalBookmark.parse( "neo4j:bookmark:v1:tx1" );
        bookmarkHolder.setBookmark( bookmark1 );
        assertEquals( Collections.singleton( bookmark1 ), bookmarkHolder.getBookmarks() );

        bookmarkHolder.setBookmark( null );
        assertEquals( Collections.singleton( bookmark1 ), bookmarkHolder.getBookmarks() );

        bookmarkHolder.setBookmark( InternalBookmark.empty() );
        assertEquals( Collections.singleton( bookmark1 ), bookmarkHolder.getBookmarks() );

        Bookmark bookmark2 = InternalBookmark.parse( "neo4j:bookmark:v1:tx2" );
        bookmarkHolder.setBookmark( bookmark2 );
        assertEquals( Collections.singleton( bookmark2 ), bookmarkHolder.getBookmarks() );

        Bookmark bookmark3 = InternalBookmark.parse( "neo4j:bookmark:v1:tx42" );
        bookmarkHolder.setBookmark( bookmark3 );
        assertEquals( Collections.singleton( bookmark3 ), bookmarkHolder.getBookmarks() );
    }

    @Test
    void bookmarkCanBeSet()
    {
        BookmarksHolder bookmarkHolder = new DefaultBookmarksHolder();
        Bookmark bookmark = InternalBookmark.parse( "neo4j:bookmark:v1:tx100" );

        bookmarkHolder.setBookmark( bookmark );

        assertEquals( Collections.singleton( bookmark ), bookmarkHolder.getBookmarks() );
    }

    @Test
    void shouldNotOverwriteBookmarkWithNull()
    {
        Bookmark initBookmark = InternalBookmark.parse( "Cat" );
        BookmarksHolder bookmarkHolder = new DefaultBookmarksHolder( Collections.singleton( initBookmark ) );
        assertEquals( Collections.singleton( initBookmark ), bookmarkHolder.getBookmarks() );
        bookmarkHolder.setBookmark( null );
        assertEquals( Collections.singleton( initBookmark ), bookmarkHolder.getBookmarks() );
    }

    @Test
    void shouldNotOverwriteBookmarkWithEmptyBookmark()
    {
        Set<Bookmark> initBookmark = Collections.singleton( InternalBookmark.parse( "Cat" ) );
        BookmarksHolder bookmarkHolder = new DefaultBookmarksHolder( initBookmark );
        assertEquals( initBookmark, bookmarkHolder.getBookmarks() );
        bookmarkHolder.setBookmark( InternalBookmark.empty() );
        assertEquals( initBookmark, bookmarkHolder.getBookmarks() );
    }
}
