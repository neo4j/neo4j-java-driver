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
 * The coarse Cypher type of a {@link Value}
 *
 * A coarse Cypher type is the type of a value that can be inferred without inspecting its elements, e.g.
 * the coarse type of a LIST OF INTEGER is LIST OF ANY?
 */
public interface CoarseCypherType extends CypherType
{
    /**
     * Test if a value has this Cypher type
     *
     * @param value the value
     * @return <tt>true</tt> if the value is a value of this Cypher type otherwise <tt>false</tt>
     */
    boolean isTypeOf( Value value );
}

