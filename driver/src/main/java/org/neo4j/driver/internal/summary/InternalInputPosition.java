/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.summary;

import org.neo4j.driver.summary.InputPosition;

/**
 * An input position refers to a specific point in a query string.
 */
public record InternalInputPosition(int offset, int line, int column) implements InputPosition {
    /**
     * Creating a position from and offset, line number and a column number.
     *
     * @param offset the offset from the start of the string, starting from 0.
     * @param line   the line number, starting from 1.
     * @param column the column number, starting from 1.
     */
    public InternalInputPosition {}

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (InternalInputPosition) o;
        return offset == that.offset && line == that.line && column == that.column;
    }

    @Override
    public String toString() {
        return "offset=" + offset + ", line=" + line + ", column=" + column;
    }
}
