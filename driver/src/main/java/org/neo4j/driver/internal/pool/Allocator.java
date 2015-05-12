/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal.pool;

import org.neo4j.driver.internal.util.Consumer;

public interface Allocator<Value>
{
    /**
     * Called when the pool needs a new value created. The 'release' handle given here will return the object to the
     * pool. How it gets invoked is up to the pooled object, but a suggested pattern is for the pooled object to
     * implement a 'close' method which calls the release handle.
     */
    Value create( Consumer<Value> release );

    /** Called when a value gets kicked out of the pool. */
    void onDispose( Value value );

    /** Called when a value gets acquired from the pool */
    void onAcquire( Value value );
}
