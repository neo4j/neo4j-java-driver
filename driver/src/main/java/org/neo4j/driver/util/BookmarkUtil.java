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
package org.neo4j.driver.util;

import java.io.Serializable;
import java.util.Collection;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.InternalBookmark;

/**
 * Utils for {@link Bookmark}. This class is deprecated and will be removed from public API in future versions.
 */
@Deprecated
public final class BookmarkUtil
{
    /**
     * Parse {@link Bookmark} from a string value. The bookmark string has to be a single bookmark string such as "aBookmarkString".
     * Use {@link BookmarkUtil#parse(Collection)} instead to parse multiple bookmark strings into one {@link Bookmark}.
     * @param value a bookmark string.
     * @return A {@link Bookmark bookmark}
     * @deprecated This method is deprecated and will be removed in future versions.
     * This method provides back-compatibility to convert string bookmarks used in previous driver versions to {@link Bookmark} which is used in this new driver version.
     * This method shall not be used to convert a bookmark string from {@link Bookmark} back to {@link Bookmark}.
     * A bookmark shall only be obtained via the driver and passed directly to the driver.
     * The content of the bookmark shall not be inspected or altered by any client application.
     */
    @Deprecated
    public static Bookmark parse( String value )
    {
        return InternalBookmark.parse( value );
    }

    /**
     * Parse {@link Bookmark} from a collection of bookmark strings. Each bookmark string in the collection has to be a single bookmark string such as "aBookmarkString".
     * Use {@link BookmarkUtil#parse(String)} instead to parse a single bookmark string into one {@link Bookmark}.
     * @param values a collections of bookmark string. The collection has to implement {@link Serializable}.
     * @return A {@link Bookmark bookmark}
     * @throws IllegalArgumentException if the collection of bookmark strings does not implement {@link Serializable}.
     * @deprecated This method is deprecated and will be removed in future versions.
     * This method provides back-compatibility to convert string bookmarks used in previous driver versions to {@link Bookmark} which is used in this new driver version.
     * This method shall not be used to convert a bookmark string from {@link Bookmark} back to {@link Bookmark}.
     * A bookmark shall only be obtained via the driver and passed directly to the driver.
     * The content of the bookmark shall not be inspected or altered by any client application.
     */
    @Deprecated
    public static Bookmark parse( Collection<String> values )
    {
        return InternalBookmark.parse( values );
    }
}
