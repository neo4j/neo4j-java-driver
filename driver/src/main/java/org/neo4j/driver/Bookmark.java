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
package org.neo4j.driver;

import java.util.Arrays;
import java.util.Set;

import org.neo4j.driver.internal.InternalBookmark;

/**
 * Causal chaining is carried out by passing bookmarks between transactions.
 *
 * When starting a session with initial bookmarks, the first transaction will be ensured to run at least after
 * the database is as up-to-date as the latest transaction referenced by the supplied bookmarks.
 *
 * Within a session, bookmark propagation is carried out automatically.
 * Thus all transactions in a session including explicit and implicit transactions are ensured to be carried out one after another.
 *
 * To opt out of this mechanism for unrelated units of work, applications can use multiple sessions.
 */
public interface Bookmark
{
    /**
     * Returns a read-only set of bookmark strings that this bookmark instance identifies.
     * This method shall only be used to serialize bookmarks.
     * @return a read-only set of bookmark strings that this bookmark instance identifies.
     */
    Set<String> values();

    /**
     * Reconstruct bookmark from de-serialized bookmarks string values.
     * This method shall not be used to create non-existing bookmarks from random string values.
     * To create a bookmark from two and more existing bookmarks, using {@link this#merge(Bookmark...)} and {@link this#merge(Iterable)} instead.
     * @param values values obtained from a previous bookmark.
     * @return A bookmark.
     */
    static Bookmark parse( Set<String> values )
    {
        return InternalBookmark.parse( values );
    }

    /**
     * Return true if the bookmark is empty.
     * @return true if the bookmark is empty.
     */
    boolean isEmpty();

    /**
     * Merge more than one bookmarks together into one bookmark.
     * @param bookmarks bookmarks to merge
     * @return Merged single bookmark.
     */
    static Bookmark merge( Bookmark... bookmarks )
    {
        return merge( Arrays.asList( bookmarks ) );
    }

    /**
     * Merge more than one bookmarks together into one bookmark.
     * @param bookmarks bookmarks to merge
     * @return Merged singled bookmark.
     */
    static Bookmark merge( Iterable<Bookmark> bookmarks )
    {
        return InternalBookmark.from( bookmarks );
    }
}
