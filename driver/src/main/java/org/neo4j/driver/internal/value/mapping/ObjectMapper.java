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

import java.util.Set;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.value.ValueException;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.Type;
import org.neo4j.driver.types.TypeSystem;

class ObjectMapper<T> implements MapAccessorMapper<T> {
    private static final TypeSystem TS = TypeSystem.getDefault();

    private static final Set<Type> SUPPORTED_VALUE_TYPES = Set.of(TS.MAP(), TS.NODE(), TS.RELATIONSHIP());

    private static final ConstructorFinder CONSTRUCTOR_FINDER = new ConstructorFinder();

    private static final ObjectInstantiator OBJECT_INSTANTIATOR = new ObjectInstantiator();

    @Override
    public boolean supports(MapAccessor mapAccessor, Class<?> targetClass) {
        return mapAccessor instanceof Value value
                ? SUPPORTED_VALUE_TYPES.contains(value.type())
                : mapAccessor instanceof org.neo4j.driver.Record;
    }

    @Override
    public T map(MapAccessor mapAccessor, Class<T> targetClass) {
        return CONSTRUCTOR_FINDER
                .findConstructor(mapAccessor, targetClass)
                .map(OBJECT_INSTANTIATOR::instantiate)
                .orElseThrow(() -> new ValueException(
                        "No suitable constructor has been found for '%s'".formatted(targetClass.getCanonicalName())));
    }
}
