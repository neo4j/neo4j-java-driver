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

import java.util.Objects;
import java.util.UUID;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;

public final class UUIDValue extends ValueAdapter {
    private final UUID val;

    public UUIDValue(UUID val) {
        if (val == null) {
            throw new IllegalArgumentException("Cannot construct StringValue from null");
        }
        this.val = val;
    }

    @Override
    public String asObject() {
        return asString();
    }

    @Override
    public UUID asUUID() {
        return val;
    }

    @Override
    public <T> T as(Class<T> targetClass) {
        if (targetClass.isAssignableFrom(UUID.class)) {
            return targetClass.cast(asUUID());
        }
        throw new Uncoercible(type().name(), targetClass.getCanonicalName());
    }

    @Override
    public String toString() {
        return val.toString();
    }

    @Override
    public Type type() {
        return InternalTypeSystem.TYPE_SYSTEM.UUID();
    }

    @Override
    public org.neo4j.bolt.connection.values.Type boltValueType() {
        return org.neo4j.bolt.connection.values.Type.UUID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var that = (UUIDValue) o;
        return Objects.equals(val, that.val);
    }

    @Override
    public int hashCode() {
        return val.hashCode();
    }
}
