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

import java.lang.reflect.Array;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.bolt.connection.values.Vector;

public abstract class AbstractArrayVector<T> implements Vector {
    private final Class<?> elementType;
    private final int length;
    protected final T elements;

    AbstractArrayVector(T elements) {
        this.elementType = elements.getClass().getComponentType();
        this.length = Array.getLength(elements);
        this.elements = arraycopy(elements);
    }

    @Override
    public Class<?> elementType() {
        return elementType;
    }

    public int length() {
        return length;
    }

    public T toArray() {
        return arraycopy(elements);
    }

    @Override
    public Object elements() {
        return elements;
    }

    protected abstract Stream<? extends Number> elementsStream();

    protected abstract String neo4jElementType();

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        var that = (AbstractArrayVector<?>) o;
        return length == that.length && Objects.equals(elementType, that.elementType) && elementsEquals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elementType, length, elementsHashCode());
    }

    @Override
    public String toString() {
        var value = elementsStream().map(Number::toString).collect(Collectors.joining(", ", "[", "]"));
        return "vector(%s, %d, %s NOT NULL)".formatted(value, length, neo4jElementType());
    }

    @SuppressWarnings({"unchecked", "SuspiciousSystemArraycopy"})
    private T arraycopy(T elements) {
        var result = (T) Array.newInstance(elementType, length);
        System.arraycopy(elements, 0, result, 0, length);
        return result;
    }

    abstract int elementsHashCode();

    abstract boolean elementsEquals(Object other);
}
