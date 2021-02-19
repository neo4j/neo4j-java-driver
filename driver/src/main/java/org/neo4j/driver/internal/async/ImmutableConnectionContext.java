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
package org.neo4j.driver.internal.async;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.spi.Connection;

import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.DatabaseNameUtil.systemDatabase;
import static org.neo4j.driver.internal.InternalBookmark.empty;

/**
 * A {@link Connection} shall fulfil this {@link ImmutableConnectionContext} when acquired from a connection provider.
 */
public class ImmutableConnectionContext implements ConnectionContext
{
    private static final ConnectionContext SINGLE_DB_CONTEXT = new ImmutableConnectionContext( defaultDatabase(), empty(), AccessMode.READ );
    private static final ConnectionContext MULTI_DB_CONTEXT = new ImmutableConnectionContext( systemDatabase(), empty(), AccessMode.READ );

    private final DatabaseName databaseName;
    private final AccessMode mode;
    private final Bookmark rediscoveryBookmark;

    public ImmutableConnectionContext( DatabaseName databaseName, Bookmark bookmark, AccessMode mode )
    {
        this.databaseName = databaseName;
        this.rediscoveryBookmark = bookmark;
        this.mode = mode;
    }

    @Override
    public DatabaseName databaseName()
    {
        return databaseName;
    }

    @Override
    public AccessMode mode()
    {
        return mode;
    }

    @Override
    public Bookmark rediscoveryBookmark()
    {
        return rediscoveryBookmark;
    }

    /**
     * A simple context is used to test connectivity with a remote server/cluster.
     * As long as there is a read only service, the connection shall be established successfully.
     * Depending on whether multidb is supported or not, this method returns different context for routing table discovery.
     */
    public static ConnectionContext simple( boolean supportsMultiDb )
    {
        return supportsMultiDb ? MULTI_DB_CONTEXT : SINGLE_DB_CONTEXT;
    }
}
