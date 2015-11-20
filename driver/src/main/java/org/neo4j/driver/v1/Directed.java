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
package org.neo4j.driver.v1;

/**
 * An item that can be considered to have <em>direction</em>.
 * This is represented by the presence of <strong>start</strong> and <strong>end</strong> attributes.
 *
 * @param <T> the type of the objects at the start and end of this directed item
 */
public interface Directed<T>
{
    /** @return the start item from this directed sequence */
    T start();

    /** @return the end item from this directed sequence */
    T end();
}
