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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.neo4j.driver.internal.util.Iterables;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.InternalBookmark.parse;

class InternalBookmarkTest
{
    @Test
    void isEmptyForEmptyBookmark()
    {
        InternalBookmark bookmark = InternalBookmark.empty();
        assertTrue( bookmark.isEmpty() );
        assertEquals( emptySet(), bookmark.values() );
    }

    @Test
    void shouldSetToEmptyForNullBookmark() throws Throwable
    {
        InternalBookmark bookmark = InternalBookmark.from( null );
        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldSetToEmptyForEmptyBookmarkIterator() throws Throwable
    {
        InternalBookmark bookmark = InternalBookmark.from( emptyList() );
        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldSetToEmptyForNullBookmarkList() throws Throwable
    {
        InternalBookmark bookmark = InternalBookmark.from( singletonList( null ) );
        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldIgnoreNullAndEmptyInBookmarkList() throws Throwable
    {
        InternalBookmark bookmark = InternalBookmark.from( Arrays.asList( InternalBookmark.empty(), null, null ) );
        assertEquals( InternalBookmark.empty(), bookmark );
    }

    @Test
    void shouldReserveBookmarkValuesCorrectly() throws Throwable
    {
        Bookmark one = parse( "one" );
        Bookmark two = parse( "two" );
        Bookmark empty = InternalBookmark.empty();
        InternalBookmark bookmark = InternalBookmark.from( Arrays.asList( one, two, null, empty ) );
        assertThat( Iterables.asList( bookmark.values() ), equalTo( Arrays.asList( "one", "two" ) ) );
    }

    @Test
    void isEmptyForNonEmptyBookmark()
    {
        InternalBookmark bookmark = InternalBookmark.parse( "SomeBookmark" );
        assertFalse( bookmark.isEmpty() );
    }

    @Test
    void asBeginTransactionParametersForNonEmptyBookmark()
    {
        InternalBookmark bookmark = InternalBookmark.parse( "SomeBookmark" );
        verifyValues( bookmark, "SomeBookmark" );
    }

    @Test
    void bookmarkFromString()
    {
        InternalBookmark bookmark = InternalBookmark.parse( "Cat" );
        assertEquals( singletonList( "Cat" ), bookmark.values() );
        verifyValues( bookmark, "Cat" );
    }

    @Test
    void bookmarkFromNullString()
    {
        InternalBookmark bookmark = InternalBookmark.parse( (String) null );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void bookmarkFromIterable()
    {
        InternalBookmark bookmark = InternalBookmark.parse( asList(
                "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" ) );
        verifyValues( bookmark, "neo4j:bookmark:v1:tx42", "neo4j:bookmark:v1:tx10", "neo4j:bookmark:v1:tx12" );
    }

    @Test
    void bookmarkFromNullIterable()
    {
        InternalBookmark bookmark = InternalBookmark.parse( (List<String>) null );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void bookmarkFromEmptyIterable()
    {
        InternalBookmark bookmark = InternalBookmark.parse( emptyList() );
        assertTrue( bookmark.isEmpty() );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithInvalidValue()
    {
        InternalBookmark bookmark = InternalBookmark.parse( asList(
                "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" ) );
        verifyValues( bookmark, "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:txcat", "neo4j:bookmark:v1:tx3" );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithEmptyStringValue()
    {
        InternalBookmark bookmark = InternalBookmark.parse( asList( "neo4j:bookmark:v1:tx9", "", "neo4j:bookmark:v1:tx3" ) );
        verifyValues( bookmark, "neo4j:bookmark:v1:tx9", "", "neo4j:bookmark:v1:tx3" );
    }

    @Test
    void asBeginTransactionParametersForBookmarkWithNullValue()
    {
        InternalBookmark bookmark = InternalBookmark.parse( asList( "neo4j:bookmark:v1:tx41", null, "neo4j:bookmark:v1:tx42" ) );
        verifyValues( bookmark, asList( "neo4j:bookmark:v1:tx41", null, "neo4j:bookmark:v1:tx42" ) );
    }

    @Test
    void shouldReturnAllBookmarks()
    {
        assertIterableEquals( emptyList(), InternalBookmark.empty().values() );
        assertIterableEquals( singletonList( "neo4j:bookmark:v1:tx42" ), InternalBookmark.parse( "neo4j:bookmark:v1:tx42" ).values() );

        List<String> bookmarks = asList( "neo4j:bookmark:v1:tx1", "neo4j:bookmark:v1:tx2", "neo4j:bookmark:v1:tx3" );
        assertIterableEquals( bookmarks, InternalBookmark.parse( bookmarks ).values() );
    }

    @Test
    void objectShouldBeTheSameWhenSerializingAndDeserializing() throws Throwable
    {
        Bookmark bookmark = InternalBookmark.parse( Arrays.asList( "neo4j:1000", "neo4j:2000" ) );

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream( outputStream );
        objectOutputStream.writeObject( bookmark );
        objectOutputStream.flush();
        objectOutputStream.close();

        ByteArrayInputStream inputStream = new ByteArrayInputStream( outputStream.toByteArray() );
        ObjectInputStream objectInputStream = new ObjectInputStream( inputStream );
        Bookmark readBookmark = (InternalBookmark) objectInputStream.readObject();
        objectInputStream.close();

        assertEquals( bookmark, readBookmark );
    }

    private static void verifyValues( InternalBookmark bookmark, String... expectedValues )
    {
        verifyValues( bookmark, asList( expectedValues ) );
    }

    private static void verifyValues( InternalBookmark bookmark, List<String> expectedValues )
    {
        assertIterableEquals( expectedValues, bookmark.values() );
    }
}
