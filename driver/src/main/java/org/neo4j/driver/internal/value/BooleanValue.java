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

import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;

public abstract class BooleanValue extends ValueAdapter {
    private BooleanValue() {
        // do nothing
    }

    public static final BooleanValue TRUE = new TrueValue();
    public static final BooleanValue FALSE = new FalseValue();

    public static BooleanValue fromBoolean(boolean value) {
        return value ? TRUE : FALSE;
    }

    @Override
    public abstract Boolean asObject();

    @Override
    public Type type() {
        return InternalTypeSystem.TYPE_SYSTEM.BOOLEAN();
    }

    @Override
    public int hashCode() {
        var value = asBoolean() ? Boolean.TRUE : Boolean.FALSE;
        return value.hashCode();
    }

    private static class TrueValue extends BooleanValue {

        @Override
        public Boolean asObject() {
            return Boolean.TRUE;
        }

        @Override
        public boolean asBoolean() {
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T as(Class<T> targetClass) {
            if (targetClass.isAssignableFrom(Boolean.class)) {
                return targetClass.cast(Boolean.TRUE);
            } else if (targetClass.isAssignableFrom(boolean.class)) {
                return (T) Boolean.TRUE;
            }
            throw new Uncoercible(type().name(), targetClass.getCanonicalName());
        }

        @Override
        public boolean isTrue() {
            return true;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(Object obj) {
            return obj == TRUE;
        }

        @Override
        public String toString() {
            return "TRUE";
        }

        @Override
        public BoltValue asBoltValue() {
            return new BoltValue(this, org.neo4j.bolt.connection.values.Type.BOOLEAN);
        }
    }

    private static class FalseValue extends BooleanValue {
        @Override
        public Boolean asObject() {
            return Boolean.FALSE;
        }

        @Override
        public boolean asBoolean() {
            return false;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T as(Class<T> targetClass) {
            if (targetClass.isAssignableFrom(Boolean.class)) {
                return targetClass.cast(Boolean.FALSE);
            } else if (targetClass.isAssignableFrom(boolean.class)) {
                return (T) Boolean.FALSE;
            }
            throw new Uncoercible(type().name(), targetClass.getCanonicalName());
        }

        @Override
        public boolean isFalse() {
            return true;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(Object obj) {
            return obj == FALSE;
        }

        @Override
        public String toString() {
            return "FALSE";
        }

        @Override
        public BoltValue asBoltValue() {
            return new BoltValue(this, org.neo4j.bolt.connection.values.Type.BOOLEAN);
        }
    }
}
