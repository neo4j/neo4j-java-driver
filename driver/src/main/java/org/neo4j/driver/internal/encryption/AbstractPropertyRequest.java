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
package org.neo4j.driver.internal.encryption;

import java.util.Objects;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Type;
import org.neo4j.driver.types.TypeSystem;

abstract class AbstractPropertyRequest {
    protected static final TypeSystem TYPE_SYSTEM = TypeSystem.getDefault();

    protected void validate(Value value) {
        Objects.requireNonNull(value);

        if (isScalarPropertyValue(value)) {
            return;
        }

        if (TYPE_SYSTEM.LIST().isTypeOf(value)) {
            validateList(value);
            return;
        }

        throw new IllegalArgumentException("Value of type " + value.type().name() + " cannot be stored as a property");
    }

    private void validateList(Value value) {
        var values = value.asList(Values::value);

        if (values.isEmpty()) {
            return;
        }

        Type elementType = null;
        PointListState pointState = null;

        for (var element : values) {
            Objects.requireNonNull(element, "list elements must not be null");
            if (TYPE_SYSTEM.NULL().isTypeOf(element)) {
                throw new IllegalArgumentException("NULL values are not allowed in lists");
            }
            if (TYPE_SYSTEM.VECTOR().isTypeOf(element)) {
                throw new IllegalArgumentException("vector values are not allowed in lists");
            }
            if (!isScalarPropertyValue(element)) {
                throw new IllegalArgumentException("only scalar property values are allowed in lists");
            }

            var currentType = element.type();
            if (elementType == null) {
                elementType = currentType;
            } else if (!elementType.equals(currentType)) {
                throw new IllegalArgumentException("LIST must be homogeneous, found both %s and %s"
                        .formatted(elementType.name(), currentType.name()));
            }

            if (TYPE_SYSTEM.POINT().isTypeOf(element)) {
                pointState = validatePoint(pointState, element.asPoint());
            }
        }
    }

    private PointListState validatePoint(PointListState state, Point point) {
        var srid = point.srid();
        var is3d = point instanceof InternalPoint3D;

        if (state == null) {
            return new PointListState(srid, is3d);
        }

        if (state.srid != srid) {
            throw new IllegalArgumentException(
                    "POINT is LIST must have the same SRID, found both %d and %d".formatted(state.srid, srid));
        }

        if (state.is3d != is3d) {
            throw new IllegalArgumentException("POINT in LIST must have the same dimensionality (2D or 3D)");
        }

        return state;
    }

    private record PointListState(int srid, boolean is3d) {}

    protected void validateAad(Value value) {
        Objects.requireNonNull(value);

        if (!isSupportedAADValue(value)) {
            throw new IllegalArgumentException("Value of type %s is not supported as AAD"
                    .formatted(value.type().name()));
        }
    }

    private boolean isScalarPropertyValue(Value value) {
        return TYPE_SYSTEM.BOOLEAN().isTypeOf(value)
                || TYPE_SYSTEM.STRING().isTypeOf(value)
                || TYPE_SYSTEM.INTEGER().isTypeOf(value)
                || TYPE_SYSTEM.FLOAT().isTypeOf(value)
                || TYPE_SYSTEM.DATE().isTypeOf(value)
                || TYPE_SYSTEM.TIME().isTypeOf(value)
                || TYPE_SYSTEM.LOCAL_TIME().isTypeOf(value)
                || TYPE_SYSTEM.LOCAL_DATE_TIME().isTypeOf(value)
                || TYPE_SYSTEM.DATE_TIME().isTypeOf(value)
                || TYPE_SYSTEM.DURATION().isTypeOf(value)
                || TYPE_SYSTEM.POINT().isTypeOf(value)
                || TYPE_SYSTEM.BYTES().isTypeOf(value)
                || TYPE_SYSTEM.VECTOR().isTypeOf(value)
                || TYPE_SYSTEM.UUID().isTypeOf(value)
                || TYPE_SYSTEM.NULL().isTypeOf(value);
    }

    private boolean isSupportedAADValue(Value value) {
        return TYPE_SYSTEM.BOOLEAN().isTypeOf(value)
                || TYPE_SYSTEM.STRING().isTypeOf(value)
                || TYPE_SYSTEM.INTEGER().isTypeOf(value)
                || TYPE_SYSTEM.DATE().isTypeOf(value)
                || TYPE_SYSTEM.TIME().isTypeOf(value)
                || TYPE_SYSTEM.LOCAL_TIME().isTypeOf(value)
                || TYPE_SYSTEM.POINT().isTypeOf(value)
                || TYPE_SYSTEM.BYTES().isTypeOf(value)
                || TYPE_SYSTEM.UUID().isTypeOf(value);
    }
}
