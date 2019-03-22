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

import java.util.List;

/**
 * The template used to configure session parameters which will be used to create a session.
 */
public interface SessionParametersTemplate
{
    /**
     * Set the initial bookmarks to be used in a session.
     * First transaction in a session will ensure that server hosting is at least as up-to-date as the
     * latest transaction referenced by the supplied bookmarks.
     *
     * @param bookmarks a series of initial bookmarks. Both {@code null} value and empty array
     * are permitted, and indicate that the bookmarks do not exist or are unknown.
     * @return this builder.
     */
    SessionParametersTemplate withBookmarks( String... bookmarks );

    /**
     * Set the initial bookmarks to be used in a session.
     * First transaction in a session will ensure that server hosting is at least as up-to-date as the
     * latest transaction referenced by the supplied bookmarks.
     *
     * @param bookmarks initial references to some previous transactions. Both {@code null} value and empty iterable
     * are permitted, and indicate that the bookmarks do not exist or are unknown.
     * @return this builder
     */
    SessionParametersTemplate withBookmarks( List<String> bookmarks );

    /**
     * Set the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     *
     * @param mode access mode.
     * @return this builder.
     */
    SessionParametersTemplate withDefaultAccessMode( AccessMode mode );

    /**
     * Set the database that the newly created session is going to connect to.
     * The given database name cannot be <code>null</code>.
     * If the database name is not set, then the default database configured on the server configuration will be connected when the session established.
     *
     * @param database the database the session going to connect to.
     * @return this builder.
     */
    SessionParametersTemplate withDatabase( String database );
}
