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
package org.neo4j.driver.net;

import org.neo4j.driver.internal.BoltServerAddress;

/**
 * Represents a host and port. Host can either be an IP address or a DNS name.
 * Both IPv4 and IPv6 hosts are supported.
 */
public interface ServerAddress
{
    /**
     * Retrieve the host portion of this {@link ServerAddress}.
     *
     * @return the host, never {@code null}.
     */
    String host();

    /**
     * Retrieve the port portion of this {@link ServerAddress}.
     *
     * @return the port, always in range [0, 65535].
     */
    int port();

    /**
     * Create a new address with the given host and port.
     *
     * @param host the host portion. Should not be {@code null}.
     * @param port the port portion. Should be in range [0, 65535].
     * @return new server address with the specified host and port.
     */
    static ServerAddress of( String host, int port )
    {
        return new BoltServerAddress( host, port );
    }
}
