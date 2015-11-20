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
 * An input position refers to a specific character in a statement.
 */
public interface InputPosition
{
    /**
     * The character offset referred to by this position; offset numbers start at 0.
     *
     * @return the offset of this position.
     */
    public int offset();

    /**
     * The line number referred to by the position; line numbers start at 1.
     *
     * @return the line number of this position.
     */
    public int line();

    /**
     * The column number referred to by the position; column numbers start at 1.
     *
     * @return the column number of this position.
     */
    public int column();
}
