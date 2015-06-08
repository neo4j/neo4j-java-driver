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
