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

import java.util.List;
import java.util.Optional;
import org.neo4j.driver.types.MapAccessor;

public class MapAccessorMapperProvider {
    private static final List<? extends MapAccessorMapper<?>> mapAccessorMappers =
            List.of(new ObjectMapper<>(), new DurationMapper());

    @SuppressWarnings("unchecked")
    public static <T> Optional<? extends MapAccessorMapper<T>> mapper(MapAccessor mapAccessor, Class<T> targetClass) {
        return (Optional<? extends MapAccessorMapper<T>>) mapAccessorMappers.stream()
                .filter(mapper -> mapper.supports(mapAccessor, targetClass))
                .findFirst();
    }
}
