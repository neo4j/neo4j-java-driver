/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import org.neo4j.driver.internal.InternalBookmark;

/**
 * Causal chaining is carried out by passing bookmarks between transactions.
 * <p>
 * When starting a session with initial bookmarks, the first transaction will be ensured to run at least after
 * the database is as up-to-date as the latest transaction referenced by the supplied bookmarks.
 * <p>
 * Within a session, bookmark propagation is carried out automatically.
 * Thus, all transactions in a session (both managed and unmanaged) are guaranteed to be carried out one after another.
 * <p>
 * To opt out of this mechanism for unrelated units of work, applications can use multiple sessions.
 */
public interface Bookmark {
    /**
     * Returns a string that this bookmark instance identifies.
     *
     * @return a string that this bookmark instance identifies.
     */
    String value();

    /**
     * Reconstruct bookmark from bookmark string value.
     *
     * @param value value obtained from a previous bookmark.
     * @return A bookmark.
     */
    static Bookmark from(String value) {
        return new InternalBookmark(value);
    }
}
