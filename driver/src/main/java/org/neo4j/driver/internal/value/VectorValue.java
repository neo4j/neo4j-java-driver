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

import org.neo4j.driver.internal.AbstractArrayVector;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.ByteVector;
import org.neo4j.driver.types.DoubleVector;
import org.neo4j.driver.types.FloatVector;
import org.neo4j.driver.types.IntVector;
import org.neo4j.driver.types.LongVector;
import org.neo4j.driver.types.ShortVector;
import org.neo4j.driver.types.Type;
import org.neo4j.driver.types.Vector;

public class VectorValue extends ObjectValueAdapter<Vector> {
    public VectorValue(Vector vector) {
        super(vector);
    }

    @Override
    public Type type() {
        return InternalTypeSystem.TYPE_SYSTEM.VECTOR();
    }

    @Override
    public org.neo4j.bolt.connection.values.Type boltValueType() {
        return org.neo4j.bolt.connection.values.Type.VECTOR;
    }

    @Override
    public <T> T as(Class<T> targetClass) {
        if (targetClass.isAssignableFrom(ByteVector.class)
                || targetClass.isAssignableFrom(ShortVector.class)
                || targetClass.isAssignableFrom(IntVector.class)
                || targetClass.isAssignableFrom(LongVector.class)
                || targetClass.isAssignableFrom(FloatVector.class)
                || targetClass.isAssignableFrom(DoubleVector.class)) {
            return targetClass.cast(asObject());
        } else if (targetClass.isArray()) {
            var arrayVector = (AbstractArrayVector<?>) asObject();
            if (targetClass.getComponentType().equals(arrayVector.elementType())) {
                return targetClass.cast(arrayVector.toArray());
            } else {
                throw new AssertionError("Unsupported type: " + targetClass);
            }
        }
        return asMapped(targetClass);
    }

    @Override
    public org.neo4j.bolt.connection.values.Vector asBoltVector() {
        var vector = (AbstractArrayVector<?>) asObject();
        return new org.neo4j.bolt.connection.values.Vector() {
            @Override
            public Class<?> elementType() {
                return vector.elementType();
            }

            @Override
            public Object elements() {
                return vector.elements();
            }
        };
    }
}
