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

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.util.Iterables;

import static java.util.Objects.requireNonNull;

public final class InternalBookmark implements Bookmark
{
    private static final InternalBookmark EMPTY = new InternalBookmark( Collections.emptySet() );

    private final Set<String> values;

    private InternalBookmark( Set<String> values )
    {
        requireNonNull( values );
        this.values = values;
    }

    public static Bookmark empty()
    {
        return EMPTY;
    }

    public static Bookmark from( Iterable<Bookmark> bookmarks )
    {
        if ( bookmarks == null )
        {
            return empty();
        }

        int size = Iterables.count( bookmarks );
        if ( size == 0 )
        {
            return empty();
        }
        else if ( size == 1 )
        {
            return from( bookmarks.iterator().next() );
        }

        Set<String> newValues = new HashSet<>();
        for ( Bookmark value : bookmarks )
        {
            if ( value == null )
            {
                continue; // skip any null bookmark value
            }
            newValues.addAll( value.values() );
        }
        return new InternalBookmark( newValues );
    }

    private static Bookmark from( Bookmark bookmark )
    {
        if ( bookmark == null )
        {
            return empty();
        }
        // it is safe to return the given bookmark as bookmarks values can not be modified once it is created.
        return bookmark;
    }

    /**
     * Used to extract bookmark from metadata from server.
     */
    public static Bookmark parse( String value )
    {
        if ( value == null )
        {
            return empty();
        }
        return new InternalBookmark( Collections.singleton( value ) );
    }

    /**
     * Used to reconstruct bookmark from values.
     */
    public static Bookmark parse( Set<String> values )
    {
        if ( values == null )
        {
            return empty();
        }
        return new InternalBookmark( values );
    }

    @Override
    public boolean isEmpty()
    {
        return values.isEmpty();
    }

    @Override
    public Set<String> values()
    {
        return Collections.unmodifiableSet( values );
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
        InternalBookmark bookmark = (InternalBookmark) o;
        return Objects.equals( values, bookmark.values );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( values );
    }

    @Override
    public String toString()
    {
        return "Bookmark{values=" + values + "}";
    }
}
