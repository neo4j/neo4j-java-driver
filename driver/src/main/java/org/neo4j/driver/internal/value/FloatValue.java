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

public class FloatValue extends NumberValueAdapter<Double> {
    private final double val;

    public FloatValue(double val) {
        this.val = val;
    }

    @Override
    public Type type() {
        return InternalTypeSystem.TYPE_SYSTEM.FLOAT();
    }

    @Override
    public Double asNumber() {
        return val;
    }

    @Override
    public long asLong() {
        var longVal = (long) val;
        if ((double) longVal != val) {
            throw new LossyCoercion(type().name(), "Java long");
        }

        return longVal;
    }

    @Override
    public int asInt() {
        var intVal = (int) val;
        if ((double) intVal != val) {
            throw new LossyCoercion(type().name(), "Java int");
        }

        return intVal;
    }

    @Override
    public double asDouble() {
        return val;
    }

    @Override
    public float asFloat() {
        var floatVal = (float) val;
        if ((double) floatVal != val) {
            throw new LossyCoercion(type().name(), "Java float");
        }

        return floatVal;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T as(Class<T> targetClass) {
        if (targetClass.equals(double.class)) {
            return (T) Double.valueOf(asDouble());
        } else if (targetClass.isAssignableFrom(Double.class)) {
            return targetClass.cast(asDouble());
        } else if (targetClass.equals(long.class)) {
            return (T) Long.valueOf(asLong());
        } else if (targetClass.equals(Long.class)) {
            return targetClass.cast(asLong());
        } else if (targetClass.equals(int.class)) {
            return (T) Integer.valueOf(asInt());
        } else if (targetClass.equals(Integer.class)) {
            return targetClass.cast(asInt());
        } else if (targetClass.equals(float.class)) {
            return (T) Float.valueOf(asFloat());
        } else if (targetClass.equals(Float.class)) {
            return targetClass.cast(asFloat());
        }
        throw new Uncoercible(type().name(), targetClass.getCanonicalName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        var values = (FloatValue) o;
        return Double.compare(values.val, val) == 0;
    }

    @Override
    public int hashCode() {
        var temp = Double.doubleToLongBits(val);
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public String toString() {
        return Double.toString(val);
    }

    @Override
    public BoltValue asBoltValue() {
        return new BoltValue(this, org.neo4j.bolt.connection.values.Type.FLOAT);
    }
}
