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
package org.neo4j.driver.internal.value.mapping;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.TypeSystem;

class DurationMapper implements MapAccessorMapper<TemporalAmount> {
    private static final TypeSystem TS = TypeSystem.getDefault();

    @Override
    public boolean supports(MapAccessor mapAccessor, Class<?> targetClass) {
        return (mapAccessor instanceof Value value && TS.DURATION().equals(value.type()))
                && (targetClass.isAssignableFrom(Period.class) || targetClass.isAssignableFrom(Duration.class));
    }

    @Override
    public TemporalAmount map(MapAccessor mapAccessor, Class<TemporalAmount> targetClass) {
        if (mapAccessor instanceof Value value) {
            var isoDuration = value.asIsoDuration();
            if (targetClass.isAssignableFrom(TemporalAmount.class)) {
                return isoDuration;
            } else if (targetClass.isAssignableFrom(Period.class)) {
                if (isoDuration.seconds() != 0 || isoDuration.nanoseconds() != 0) {
                    throw new LossyCoercion(value.type().name(), targetClass.getCanonicalName());
                }
                var totalMonths = isoDuration.months();
                var splitYears = totalMonths / 12;
                var splitMonths = totalMonths % 12;
                try {
                    return Period.of(
                                    Math.toIntExact(splitYears),
                                    Math.toIntExact(splitMonths),
                                    Math.toIntExact(isoDuration.days()))
                            .normalized();
                } catch (ArithmeticException e) {
                    throw new LossyCoercion(value.type().name(), targetClass.getCanonicalName());
                }
            } else if (targetClass.isAssignableFrom(Duration.class)) {
                if (isoDuration.months() != 0 || isoDuration.days() != 0) {
                    throw new LossyCoercion(value.type().name(), targetClass.getCanonicalName());
                }
                return Duration.ofSeconds(isoDuration.seconds()).plusNanos(isoDuration.nanoseconds());
            }
            throw new Uncoercible(value.type().name(), targetClass.getCanonicalName());
        } else {
            throw new Uncoercible(mapAccessor.getClass().getCanonicalName(), targetClass.getCanonicalName());
        }
    }
}
