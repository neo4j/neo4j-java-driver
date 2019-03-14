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
import java.util.List;
import java.util.Objects;

import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

/**
 * Describes the session attributes.
 */
public class SessionParameters
{
    private static final SessionParameters EMPTY = builder().build();

    private final List<String> bookmarks;
    private final AccessMode accessMode;
    private final String databaseName;

    private SessionParameters( Builder builder )
    {
        this.bookmarks = builder.bookmarks;
        this.accessMode = builder.accessMode;
        this.databaseName = builder.databaseName;
    }

    /**
     * Returns the initial bookmarks.
     * First transaction in the session created with this {@link SessionParameters}
     * will ensure that server hosting is at least as up-to-date as the
     * latest transaction referenced by the supplied initial bookmarks.
     * @return the initial bookmarks.
     */
    public List<String> bookmark()
    {
        return bookmarks;
    }

    /**
     * The type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     * @return the access mode.
     */
    public AccessMode accessMode()
    {
        return accessMode;
    }

    /**
     * The database where the session is going to connect to.
     * @return the database name where the session is going to connect to.
     */
    public String databaseName()
    {
        return databaseName;
    }

    /**
     * Creates a session parameter builder.
     * @return a session parameter builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Returns a static {@link SessionParameters} with default values for a general purpose session.
     * @return a session parameter for a general purpose session.
     */
    public static SessionParameters empty()
    {
        return EMPTY;
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
        return Objects.equals( bookmarks, that.bookmarks ) && accessMode == that.accessMode && databaseName.equals( that.databaseName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( bookmarks, accessMode, databaseName );
    }

    @Override
    public String toString()
    {
        return "SessionParameters{" + "bookmarks=" + bookmarks + ", accessMode=" + accessMode + ", databaseName='" + databaseName + '\'' + '}';
    }

    /**
     * The builder of {@link SessionParameters}.
     */
    public static class Builder
    {
        private List<String> bookmarks = null;
        private AccessMode accessMode = AccessMode.WRITE;
        private String databaseName = ABSENT_DB_NAME;

        private Builder()
        {
        }

        /**
         * Set the initial bookmarks to be used in a session.
         * First transaction in a session will ensure that server hosting is at least as up-to-date as the
         * latest transaction referenced by the supplied bookmarks.
         * @param bookmarks a series of initial bookmarks. Both {@code null} value and empty array
         * are permitted, and indicate that the bookmarks do not exist or are unknown.
         * @return this builder.
         */
        public Builder withBookmark( String... bookmarks )
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

        /**
         * Set the initial bookmarks to be used in a session.
         * First transaction in a session will ensure that server hosting is at least as up-to-date as the
         * latest transaction referenced by the supplied bookmarks.
         * @param bookmarks initial references to some previous transactions. Both {@code null} value and empty iterable
         * are permitted, and indicate that the bookmarks do not exist or are unknown.
         * @return this builder
         */
        public Builder withBookmark( List<String> bookmarks )
        {
            this.bookmarks = bookmarks;
            return this;
        }

        /**
         * Set the type of access required by units of work in this session,
         * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
         * @param mode access mode.
         * @return this builder.
         */
        public Builder withAccessMode( AccessMode mode )
        {
            this.accessMode = mode;
            return this;
        }

        /**
         * Set the name of the database that the newly created session is going to connect to.
         * The given database name cannot be <code>null</code>.
         * If the database name is not set, then the default database will be connected when the session established.
         * @param databaseName the database name the session going to connect to.
         * @return this builder.
         */
        public Builder withDatabaseName( String databaseName )
        {
            Objects.requireNonNull( databaseName, "Database name cannot be null." );
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Build the {@link SessionParameters} to pass in when creating a session.
         * @return a new {@link SessionParameters}
         */
        public SessionParameters build()
        {
            return new SessionParameters( this );
        }
    }
}
