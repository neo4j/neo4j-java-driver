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
package org.neo4j.driver.internal.async;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.internal.spi.Connection;

import static org.neo4j.driver.internal.InternalBookmark.empty;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

/**
 * A {@link Connection} shall fulfil this {@link ImmutableConnectionContext} when acquired from a connection provider.
 */
public class ImmutableConnectionContext implements ConnectionContext
{
    private static final ConnectionContext SIMPLE = new ImmutableConnectionContext( ABSENT_DB_NAME, empty(), AccessMode.READ );

    private final String databaseName;
    private final AccessMode mode;
    private final InternalBookmark rediscoveryBookmark;

    public ImmutableConnectionContext( String databaseName, InternalBookmark bookmark, AccessMode mode )
    {
        this.databaseName = databaseName;
        this.rediscoveryBookmark = bookmark;
        this.mode = mode;
    }

    @Override
    public String databaseName()
    {
        return databaseName;
    }

    @Override
    public AccessMode mode()
    {
        return mode;
    }

    @Override
    public InternalBookmark rediscoveryBookmark()
    {
        return rediscoveryBookmark;
    }

    /**
     * A simple context is used to test connectivity with a remote server/cluster.
     * As long as there is a read only service, the connection shall be established successfully.
     * This context should be applicable for both bolt v4 and bolt v3 routing table rediscovery.
     */
    public static ConnectionContext simple()
    {
        return SIMPLE;
    }
}
