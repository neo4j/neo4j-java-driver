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
package org.neo4j.driver.internal.value;

import java.lang.reflect.ParameterizedType;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;

public final class NullValue extends ValueAdapter {
    public static final Value NULL = new NullValue();

    private NullValue() {}

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public Object asObject() {
        return null;
    }

    @Override
    public String asString() {
        return "null";
    }

    @Override
    public <T> T as(Class<T> targetClass) {
        return null;
    }

    @Override
    public Object as(ParameterizedType type) {
        return null;
    }

    @Override
    public Type type() {
        return InternalTypeSystem.TYPE_SYSTEM.NULL();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object obj) {
        return obj == NULL;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "NULL";
    }

    @Override
    public BoltValue asBoltValue() {
        return new BoltValue(this, org.neo4j.bolt.connection.values.Type.NULL);
    }
}
