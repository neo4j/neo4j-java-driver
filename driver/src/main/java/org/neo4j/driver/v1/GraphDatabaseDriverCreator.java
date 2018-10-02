/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1;

import java.net.URI;
import java.util.Collection;

/**
 * Helps in creating {@link Driver drivers}.
 *
 * @see Driver
 * @see #createDriver()
 * @since 1.7
 */
public class GraphDatabaseDriverCreator {

    private final Collection<URI> uris;
    private final AuthToken authToken;
    private final Config config;

    GraphDatabaseDriverCreator(Collection<URI> uris) {
        this(uris, AuthTokens.none(), Config.defaultConfig());
    }

    GraphDatabaseDriverCreator(Collection<URI> uris, AuthToken authToken, Config config) {
        this.uris = uris;
        this.authToken = authToken;
        this.config = config;
    }

    /**
     * <p>Override default {@link AuthToken auth token}.</p>
     *
     * Existing instance of the {@link GraphDatabaseDriverCreator driver creator} is cloned.
     * Mutation of this class is not supported.
     *
     * @see AuthTokens#none()
     * @param authToken {@link AuthToken token} to be used for authentication
     * @return new instance of {@link GraphDatabaseDriverCreator this class}
     */
    public GraphDatabaseDriverCreator withToken(AuthToken authToken) {
        return new GraphDatabaseDriverCreator(this.uris, authToken, this.config);
    }

    /**
     * <p>Override default {@link Config config}.</p>
     *
     * Existing instance of the {@link GraphDatabaseDriverCreator driver creator} is cloned.
     * Mutation of this class is not supported.
     *
     * @see Config#defaultConfig()
     * @param config {@link Config config} to be used for the driver connection
     * @return new instance of {@link GraphDatabaseDriverCreator this class}
     */
    public GraphDatabaseDriverCreator withConfig(Config config) {
        return new GraphDatabaseDriverCreator(this.uris, this.authToken, config);
    }

    /**
     * <p>Create an instance of a driver with the current settings</p>
     *
     * <p>If a single URI was provided, {@link GraphDatabase#driver(URI, AuthToken, Config)} is invoked.</p>
     * <p>If a collection of URIs were provided, {@link GraphDatabase#routingDriver(Iterable, AuthToken, Config)} will be invoked</p>
     *
     * @see GraphDatabase#driver(URI, AuthToken, Config)
     * @see GraphDatabase#routingDriver(Iterable, AuthToken, Config)
     * @return new instance of {@link GraphDatabaseDriverCreator this class}
     */
    public Driver createDriver() {
        if (uris.size() > 1) {
            return GraphDatabase.routingDriver(uris, authToken, config);
        } else {
            return GraphDatabase.driver(uris.iterator().next(), authToken, config);
        }
    }
}
