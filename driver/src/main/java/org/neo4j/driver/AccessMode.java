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
package org.neo4j.driver;

/**
 * Used by Routing Driver to decide if a transaction should be routed to a write server or a read server in a cluster.
 * When running a transaction, a write transaction requires a server that supports writes.
 * A read transaction, on the other hand, requires a server that supports read operations.
 * This classification is key for routing driver to route transactions to a cluster correctly.
 *
 * While any {@link AccessMode} will be ignored while running transactions via a driver towards a single server.
 * As the single server serves both read and write operations at the same time.
 */
public enum AccessMode
{
    /**
     * Use this for transactions that requires a read server in a cluster
     */
    READ,
    /**
     * Use this for transactions that requires a write server in a cluster
     */
    WRITE
}
