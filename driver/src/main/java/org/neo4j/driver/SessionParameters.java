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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

/**
 * The session parameters used to configure a session.
 */
public class SessionParameters implements Cloneable
{
    private List<String> bookmarks = null;
    private AccessMode defaultAccessMode = AccessMode.WRITE;
    private String database = ABSENT_DB_NAME;

    public SessionParameters()
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
    public SessionParameters withBookmarks( String... bookmarks )
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
    public SessionParameters withBookmarks( List<String> bookmarks )
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
    public SessionParameters withDefaultAccessMode( AccessMode mode )
    {
        this.defaultAccessMode = mode;
        return this;
    }

    /**
     * Set the database that the newly created session is going to connect to.
     * The given database name cannot be <code>null</code>.
     * If the database name is not set, then the default database configured on the server configuration will be connected when the session established.
     * @param database the database the session going to connect to.
     * @return this builder.
     */
    public SessionParameters withDatabase( String database )
    {
        Objects.requireNonNull( database, "Database cannot be null." );
        this.database = database;
        return this;
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

    public SessionParameters clone()
    {
        SessionParameters clone = null;
        try
        {
            clone = (SessionParameters) super.clone();
        }
        catch ( CloneNotSupportedException e )
        {
            throw new IllegalStateException( "Failed to clone session parameters.", e );
        }

        if ( clone.bookmarks != null )
        {
            clone.withBookmarks( new ArrayList<>( clone.bookmarks ) ); // we take a deep copy of bookmark arrays
        }
        return clone;
    }
}
