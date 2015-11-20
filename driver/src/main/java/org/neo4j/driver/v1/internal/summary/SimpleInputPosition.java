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
package org.neo4j.driver.v1.internal.summary;

import org.neo4j.driver.v1.InputPosition;

/**
 * An input position refers to a specific point in a query string.
 */
public class SimpleInputPosition implements InputPosition
{
    private final int offset;
    private final int line;
    private final int column;

    /**
     * Creating a position from and offset, line number and a column number.
     *
     * @param offset the offset from the start of the string, starting from 0.
     * @param line the line number, starting from 1.
     * @param column the column number, starting from 1.
     */
    public SimpleInputPosition( int offset, int line, int column )
    {
        this.offset = offset;
        this.line = line;
        this.column = column;
    }

    @Override
    public int offset()
    {
        return offset;
    }

    @Override
    public int line()
    {
        return line;
    }

    @Override
    public int column()
    {
        return column;
    }

    @Override
    public String toString()
    {
        return "offset=" + offset + ", line=" + line + ", column=" + column;
    }
}
