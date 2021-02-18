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

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A resolver function used by the routing driver to resolve the initial address used to create the driver.
 */
@FunctionalInterface
public interface ServerAddressResolver
{
    /**
     * Resolve the given address to a set of other addresses.
     * It is highly recommended to shuffle the addresses returned to prevent the driver from
     * always retrying servers in a specific order.
     * Considering returning a {@link LinkedHashSet} to reserve the iteration order of a set.
     * Exceptions thrown by this method will be logged and driver will continue using the original address.
     *
     * @param address the address to resolve.
     * @return new set of addresses.
     */
    Set<ServerAddress> resolve( ServerAddress address );
}
