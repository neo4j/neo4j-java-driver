/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.cluster;

import java.util.Collections;
import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static java.util.Collections.unmodifiableSet;

public class RoutingTableChange
{
    public static final RoutingTableChange EMPTY = new RoutingTableChange(
            Collections.<BoltServerAddress>emptySet(), Collections.<BoltServerAddress>emptySet() );

    private final Set<BoltServerAddress> added;
    private final Set<BoltServerAddress> removed;

    public RoutingTableChange( Set<BoltServerAddress> added, Set<BoltServerAddress> removed )
    {
        this.added = added;
        this.removed = removed;
    }

    public Set<BoltServerAddress> added()
    {
        return unmodifiableSet( added );
    }

    public Set<BoltServerAddress> removed()
    {
        return unmodifiableSet( removed );
    }

    @Override
    public String toString()
    {
        return "RoutingTableChange{" +
               "added=" + added +
               ", removed=" + removed +
               '}';
    }
}
