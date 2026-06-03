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
package org.neo4j.driver.internal;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.neo4j.driver.types.Int16Vector;

public final class InternalInt16Vector extends AbstractArrayVector<short[]> implements Int16Vector {
    public InternalInt16Vector(short[] elements) {
        super(elements);
    }

    @Override
    protected Stream<? extends Number> elementsStream() {
        return IntStream.range(0, this.elements.length).mapToObj(i -> this.elements[i]);
    }

    @Override
    protected String neo4jElementType() {
        return "INTEGER16";
    }

    @Override
    int elementsHashCode() {
        return Arrays.hashCode(elements);
    }

    @Override
    boolean elementsEquals(Object other) {
        return Arrays.equals(elements, ((InternalInt16Vector) other).elements);
    }
}
