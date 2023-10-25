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
package org.neo4j.driver.internal.async;

import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.DatabaseNameUtil.systemDatabase;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.spi.Connection;

/**
 * A {@link Connection} shall fulfil this {@link ImmutableConnectionContext} when acquired from a connection provider.
 */
public class ImmutableConnectionContext implements ConnectionContext {
    private static final ConnectionContext SINGLE_DB_CONTEXT =
            new ImmutableConnectionContext(defaultDatabase(), Collections.emptySet(), AccessMode.READ);
    private static final ConnectionContext MULTI_DB_CONTEXT =
            new ImmutableConnectionContext(systemDatabase(), Collections.emptySet(), AccessMode.READ);

    private final CompletableFuture<DatabaseName> databaseNameFuture;
    private final AccessMode mode;
    private final Set<Bookmark> rediscoveryBookmarks;

    public ImmutableConnectionContext(DatabaseName databaseName, Set<Bookmark> bookmarks, AccessMode mode) {
        this.databaseNameFuture = CompletableFuture.completedFuture(databaseName);
        this.rediscoveryBookmarks = bookmarks;
        this.mode = mode;
    }

    @Override
    public CompletableFuture<DatabaseName> databaseNameFuture() {
        return databaseNameFuture;
    }

    @Override
    public AccessMode mode() {
        return mode;
    }

    @Override
    public Set<Bookmark> rediscoveryBookmarks() {
        return rediscoveryBookmarks;
    }

    @Override
    public String impersonatedUser() {
        return null;
    }

    @Override
    public AuthToken overrideAuthToken() {
        return null;
    }

    /**
     * A simple context is used to test connectivity with a remote server/cluster. As long as there is a read only service, the connection shall be established
     * successfully. Depending on whether multidb is supported or not, this method returns different context for routing table discovery.
     */
    public static ConnectionContext simple(boolean supportsMultiDb) {
        return supportsMultiDb ? MULTI_DB_CONTEXT : SINGLE_DB_CONTEXT;
    }
}
