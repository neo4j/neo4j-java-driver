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

import org.neo4j.driver.Bookmark;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultBookmarkHolderTest
{
    @Test
    void shouldAllowToGetAndSetBookmarks()
    {
        BookmarkHolder bookmarkHolder = new DefaultBookmarkHolder();
        assertEquals( InternalBookmark.empty(), bookmarkHolder.getBookmark() );

        bookmarkHolder.setBookmark( null );
        assertEquals( InternalBookmark.empty(), bookmarkHolder.getBookmark() );

        bookmarkHolder.setBookmark( InternalBookmark.empty() );
        assertEquals( InternalBookmark.empty(), bookmarkHolder.getBookmark() );

        Bookmark bookmark1 = InternalBookmark.parse( "neo4j:bookmark:v1:tx1" );
        bookmarkHolder.setBookmark( bookmark1 );
        assertEquals( bookmark1, bookmarkHolder.getBookmark() );

        bookmarkHolder.setBookmark( null );
        assertEquals( bookmark1, bookmarkHolder.getBookmark() );

        bookmarkHolder.setBookmark( InternalBookmark.empty() );
        assertEquals( bookmark1, bookmarkHolder.getBookmark() );

        Bookmark bookmark2 = InternalBookmark.parse( "neo4j:bookmark:v1:tx2" );
        bookmarkHolder.setBookmark( bookmark2 );
        assertEquals( bookmark2, bookmarkHolder.getBookmark() );

        Bookmark bookmark3 = InternalBookmark.parse( "neo4j:bookmark:v1:tx42" );
        bookmarkHolder.setBookmark( bookmark3 );
        assertEquals( bookmark3, bookmarkHolder.getBookmark() );
    }

    @Test
    void bookmarkCanBeSet()
    {
        BookmarkHolder bookmarkHolder = new DefaultBookmarkHolder();
        Bookmark bookmark = InternalBookmark.parse( "neo4j:bookmark:v1:tx100" );

        bookmarkHolder.setBookmark( bookmark );

        assertEquals( bookmark, bookmarkHolder.getBookmark() );
    }

    @Test
    void shouldNotOverwriteBookmarkWithNull()
    {
        Bookmark initBookmark = InternalBookmark.parse( "Cat" );
        BookmarkHolder bookmarkHolder = new DefaultBookmarkHolder( initBookmark );
        assertEquals( initBookmark, bookmarkHolder.getBookmark() );
        bookmarkHolder.setBookmark( null );
        assertEquals( initBookmark, bookmarkHolder.getBookmark() );
    }

    @Test
    void shouldNotOverwriteBookmarkWithEmptyBookmark()
    {
        Bookmark initBookmark = InternalBookmark.parse( "Cat" );
        BookmarkHolder bookmarkHolder = new DefaultBookmarkHolder( initBookmark );
        assertEquals( initBookmark, bookmarkHolder.getBookmark() );
        bookmarkHolder.setBookmark( InternalBookmark.empty() );
        assertEquals( initBookmark, bookmarkHolder.getBookmark() );
    }
}
