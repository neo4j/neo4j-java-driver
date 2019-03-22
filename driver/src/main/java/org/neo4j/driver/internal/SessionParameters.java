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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.SessionParametersTemplate;

import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

/**
 * The session parameters used to configure a session.
 */
public class SessionParameters
{
    private static final SessionParameters EMPTY = template().build();

    private final List<String> bookmarks;
    private final AccessMode defaultAccessMode;
    private final String database;

    /**
     * Creates a session parameter template.
     * @return a session parameter template.
     */
    public static Template template()
    {
        return new Template();
    }

    /**
     * Returns a static {@link SessionParameters} with default values for a general purpose session.
     * @return a session parameter for a general purpose session.
     */
    public static SessionParameters empty()
    {
        return EMPTY;
    }

    private SessionParameters( Template template )
    {
        this.bookmarks = template.bookmarks;
        this.defaultAccessMode = template.defaultAccessMode;
        this.database = template.database;
    }

    /**
     * Returns the initial bookmarks.
     * First transaction in the session created with this {@link SessionParameters}
     * will ensure that server hosting is at least as up-to-date as the
     * latest transaction referenced by the supplied initial bookmarks.
     * @return the initial bookmarks.
     */
    public List<String> bookmarks()
    {
        return bookmarks;
    }

    /**
     * The type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     * @return the access mode.
     */
    public AccessMode defaultAccessMode()
    {
        return defaultAccessMode;
    }

    /**
     * The database where the session is going to connect to.
     * @return the database name where the session is going to connect to.
     */
    public String database()
    {
        return database;
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
        SessionParameters that = (SessionParameters) o;
        return Objects.equals( bookmarks, that.bookmarks ) && defaultAccessMode == that.defaultAccessMode && database.equals( that.database );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( bookmarks, defaultAccessMode, database );
    }

    @Override
    public String toString()
    {
        return "SessionParameters{" + "bookmarks=" + bookmarks + ", defaultAccessMode=" + defaultAccessMode + ", database='" + database + '\'' + '}';
    }

    public static class Template implements SessionParametersTemplate
    {
        private List<String> bookmarks = null;
        private AccessMode defaultAccessMode = AccessMode.WRITE;
        private String database = ABSENT_DB_NAME;

        private Template()
        {
        }

        @Override
        public Template withBookmarks( String... bookmarks )
        {
            if ( bookmarks == null )
            {
                this.bookmarks = null;
            }
            else
            {
                this.bookmarks = Arrays.asList( bookmarks );
            }
            return this;
        }

        @Override
        public Template withBookmarks( List<String> bookmarks )
        {
            this.bookmarks = bookmarks;
            return this;
        }

        @Override
        public Template withDefaultAccessMode( AccessMode mode )
        {
            this.defaultAccessMode = mode;
            return this;
        }

        @Override
        public Template withDatabase( String database )
        {
            Objects.requireNonNull( database, "Database cannot be null." );
            this.database = database;
            return this;
        }

        SessionParameters build()
        {
            return new SessionParameters( this );
        }
    }
}
