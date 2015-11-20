/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1.internal.spi;

import java.net.URI;
import java.util.Collection;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * A Connector conducts the client side of a client-server dialogue,
 * along with its server side counterpart, the Listener.
 */
public interface Connector
{
    /**
     * Determine whether this connector can support the sessionURL specified.
     *
     * @param scheme a URL scheme
     * @return true if this scheme is supported, false otherwise
     */
    boolean supports( String scheme );

    /**
     * Establish a connection to a remote listener and attach to the session identified.
     *
     * @param sessionURL a URL identifying a remote session
     * @param config a configuration for this connection
     * @return a Connection object
     */
    Connection connect( URI sessionURL, Config config ) throws ClientException;

    /** List names of supported schemes, used for error messages and similar signaling to end users. */
    Collection<String> supportedSchemes();
}
