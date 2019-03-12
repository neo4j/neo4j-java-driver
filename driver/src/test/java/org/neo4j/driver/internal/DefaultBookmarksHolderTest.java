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

import static org.junit.jupiter.api.Assertions.assertEquals;

class DefaultBookmarksHolderTest
{
    @Test
    void shouldAllowToGetAndSetBookmarks()
    {
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder();
        assertEquals( Bookmarks.empty(), bookmarksHolder.getBookmarks() );

        bookmarksHolder.setBookmarks( null );
        assertEquals( Bookmarks.empty(), bookmarksHolder.getBookmarks() );

        bookmarksHolder.setBookmarks( Bookmarks.empty() );
        assertEquals( Bookmarks.empty(), bookmarksHolder.getBookmarks() );

        Bookmarks bookmarks1 = Bookmarks.from( "neo4j:bookmark:v1:tx1" );
        bookmarksHolder.setBookmarks( bookmarks1 );
        assertEquals( bookmarks1, bookmarksHolder.getBookmarks() );

        bookmarksHolder.setBookmarks( null );
        assertEquals( bookmarks1, bookmarksHolder.getBookmarks() );

        bookmarksHolder.setBookmarks( Bookmarks.empty() );
        assertEquals( bookmarks1, bookmarksHolder.getBookmarks() );

        Bookmarks bookmarks2 = Bookmarks.from( "neo4j:bookmark:v1:tx2" );
        bookmarksHolder.setBookmarks( bookmarks2 );
        assertEquals( bookmarks2, bookmarksHolder.getBookmarks() );

        Bookmarks bookmarks3 = Bookmarks.from( "neo4j:bookmark:v1:tx42" );
        bookmarksHolder.setBookmarks( bookmarks3 );
        assertEquals( bookmarks3, bookmarksHolder.getBookmarks() );
    }

    @Test
    void bookmarkCanBeSet()
    {
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder();
        Bookmarks bookmarks = Bookmarks.from( "neo4j:bookmark:v1:tx100" );

        bookmarksHolder.setBookmarks( bookmarks );

        assertEquals( bookmarks.maxBookmarkAsString(), bookmarksHolder.lastBookmark() );
    }

    @Test
    void shouldNotOverwriteBookmarkWithNull()
    {
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder( Bookmarks.from( "Cat" ) );
        assertEquals( "Cat", bookmarksHolder.lastBookmark() );
        bookmarksHolder.setBookmarks( null );
        assertEquals( "Cat", bookmarksHolder.lastBookmark() );
    }

    @Test
    void shouldNotOverwriteBookmarkWithEmptyBookmark()
    {
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder( Bookmarks.from( "Cat" ) );
        assertEquals( "Cat", bookmarksHolder.lastBookmark() );
        bookmarksHolder.setBookmarks( Bookmarks.empty() );
        assertEquals( "Cat", bookmarksHolder.lastBookmark() );
    }

    @Test
    void setLastBookmark()
    {
        BookmarksHolder bookmarksHolder = new DefaultBookmarksHolder();

        bookmarksHolder.setBookmarks( Bookmarks.from( "TheBookmark" ) );

        assertEquals( "TheBookmark", bookmarksHolder.lastBookmark() );
    }
}