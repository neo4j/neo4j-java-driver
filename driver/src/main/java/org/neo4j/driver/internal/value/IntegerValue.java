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

import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;

public class IntegerValue extends NumberValueAdapter<Long> {
    private final long val;

    public IntegerValue(long val) {
        this.val = val;
    }

    @Override
    public Type type() {
        return InternalTypeSystem.TYPE_SYSTEM.INTEGER();
    }

    @Override
    public Long asNumber() {
        return val;
    }

    @Override
    public long asLong() {
        return val;
    }

    @Override
    public int asInt() {
        if (val > Integer.MAX_VALUE || val < Integer.MIN_VALUE) {
            throw new LossyCoercion(type().name(), "Java int");
        }
        return (int) val;
    }

    @Override
    public double asDouble() {
        var doubleVal = (double) val;
        if ((long) doubleVal != val) {
            throw new LossyCoercion(type().name(), "Java double");
        }

        return (double) val;
    }

    @Override
    public float asFloat() {
        return (float) val;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T as(Class<T> targetClass) {
        if (targetClass.equals(long.class)) {
            return (T) Long.valueOf(asLong());
        } else if (targetClass.isAssignableFrom(Long.class)) {
            return targetClass.cast(asLong());
        } else if (targetClass.equals(int.class)) {
            return (T) Integer.valueOf(asInt());
        } else if (targetClass.equals(Integer.class)) {
            return targetClass.cast(asInt());
        } else if (targetClass.equals(double.class)) {
            return (T) Double.valueOf(asDouble());
        } else if (targetClass.equals(Double.class)) {
            return targetClass.cast(asDouble());
        } else if (targetClass.equals(float.class)) {
            return (T) Float.valueOf(asFloat());
        } else if (targetClass.equals(Float.class)) {
            return targetClass.cast(asFloat());
        } else if (targetClass.equals(short.class)) {
            return (T) Short.valueOf(asShort());
        } else if (targetClass.equals(Short.class)) {
            return targetClass.cast(asShort());
        }
        throw new Uncoercible(type().name(), targetClass.getCanonicalName());
    }

    private short asShort() {
        if (val > Short.MAX_VALUE || val < Short.MIN_VALUE) {
            throw new LossyCoercion(type().name(), "Java short");
        }
        return (short) val;
    }

    @Override
    public String toString() {
        return Long.toString(val);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        var values = (IntegerValue) o;
        return val == values.val;
    }

    @Override
    public int hashCode() {
        return (int) (val ^ (val >>> 32));
    }

    @Override
    public BoltValue asBoltValue() {
        return new BoltValue(this, org.neo4j.bolt.connection.values.Type.INTEGER);
    }
}
