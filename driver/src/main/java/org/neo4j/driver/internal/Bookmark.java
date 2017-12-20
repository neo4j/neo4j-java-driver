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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.neo4j.driver.v1.Value;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.neo4j.driver.internal.util.Iterables.newHashMapWithSize;
import static org.neo4j.driver.v1.Values.value;

public final class Bookmark
{
    private static final String BOOKMARK_KEY = "bookmark";
    private static final String BOOKMARKS_KEY = "bookmarks";
    private static final String BOOKMARK_PREFIX = "neo4j:bookmark:v1:tx";

    private static final long UNKNOWN_BOOKMARK_VALUE = -1;

    private static final Bookmark EMPTY = new Bookmark( Collections.<String>emptySet() );

    private final Iterable<String> values;
    private final String maxValue;

    private Bookmark( Iterable<String> values )
    {
        this.values = values;
        this.maxValue = maxBookmark( values );
    }

    public static Bookmark empty()
    {
        return EMPTY;
    }

    public static Bookmark from( String value )
    {
        if ( value == null )
        {
            return empty();
        }
        return from( singleton( value ) );
    }

    public static Bookmark from( Iterable<String> values )
    {
        if ( values == null )
        {
            return empty();
        }
        return new Bookmark( values );
    }

    public boolean isEmpty()
    {
        return maxValue == null;
    }

    public String maxBookmarkAsString()
    {
        return maxValue;
    }

    public Map<String,Value> asBeginTransactionParameters()
    {
        if ( isEmpty() )
        {
            return emptyMap();
        }

        // Driver sends {bookmark: "max", bookmarks: ["one", "two", "max"]} instead of simple
        // {bookmarks: ["one", "two", "max"]} for backwards compatibility reasons. Old servers can only accept single
        // bookmark that is why driver has to parse and compare given list of bookmarks. This functionality will
        // eventually be removed.
        Map<String,Value> parameters = newHashMapWithSize( 2 );
        parameters.put( BOOKMARK_KEY, value( maxValue ) );
        parameters.put( BOOKMARKS_KEY, value( values ) );
        return parameters;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        Bookmark bookmark = (Bookmark) o;
        return Objects.equals( values, bookmark.values ) &&
               Objects.equals( maxValue, bookmark.maxValue );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( values, maxValue );
    }

    @Override
    public String toString()
    {
        return "Bookmark{values=" + values + "}";
    }

    private static String maxBookmark( Iterable<String> bookmarks )
    {
        if ( bookmarks == null )
        {
            return null;
        }

        Iterator<String> iterator = bookmarks.iterator();

        if ( !iterator.hasNext() )
        {
            return null;
        }

        String maxBookmark = iterator.next();
        long maxValue = bookmarkValue( maxBookmark );

        while ( iterator.hasNext() )
        {
            String bookmark = iterator.next();
            long value = bookmarkValue( bookmark );

            if ( value > maxValue )
            {
                maxBookmark = bookmark;
                maxValue = value;
            }
        }

        return maxBookmark;
    }

    private static long bookmarkValue( String value )
    {
        if ( value != null && value.startsWith( BOOKMARK_PREFIX ) )
        {
            try
            {
                return Long.parseLong( value.substring( BOOKMARK_PREFIX.length() ) );
            }
            catch ( NumberFormatException e )
            {
                return UNKNOWN_BOOKMARK_VALUE;
            }
        }
        return UNKNOWN_BOOKMARK_VALUE;
    }
}
