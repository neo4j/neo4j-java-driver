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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.mapping.Property;
import org.neo4j.driver.types.MapAccessor;

class ConstructorFinder {
    @SuppressWarnings("unchecked")
    public <T> Optional<ObjectMetadata<T>> findConstructor(MapAccessor mapAccessor, Class<T> targetClass) {
        PropertiesMatch<T> bestPropertiesMatch = null;
        var constructors = targetClass.getDeclaredConstructors();
        var propertyNamesSize = mapAccessor.size();
        for (var constructor : constructors) {
            var accessible = false;
            try {
                accessible = constructor.canAccess(null);
            } catch (Throwable e) {
                // ignored
            }
            if (!accessible) {
                continue;
            }
            var matchNumbers = matchPropertyNames(mapAccessor, constructor);
            if (bestPropertiesMatch == null
                    || (matchNumbers.match() >= bestPropertiesMatch.match()
                            && matchNumbers.mismatch() < bestPropertiesMatch.mismatch())) {
                bestPropertiesMatch = (PropertiesMatch<T>) matchNumbers;
                if (bestPropertiesMatch.match() == propertyNamesSize && bestPropertiesMatch.mismatch() == 0) {
                    break;
                }
            }
        }
        if (bestPropertiesMatch == null || bestPropertiesMatch.match() == 0) {
            return Optional.empty();
        }

        return Optional.of(new ObjectMetadata<>(bestPropertiesMatch.constructor(), bestPropertiesMatch.arguments()));
    }

    private <T> PropertiesMatch<T> matchPropertyNames(MapAccessor mapAccessor, Constructor<T> constructor) {
        var match = 0;
        var mismatch = 0;
        var parameters = constructor.getParameters();
        var arguments = new ArrayList<Argument>(parameters.length);
        for (var parameter : parameters) {
            var propertyNameAnnotation = parameter.getAnnotation(Property.class);
            var propertyName = propertyNameAnnotation != null ? propertyNameAnnotation.value() : parameter.getName();
            var value = mapAccessor.get(propertyName);
            if (value != null) {
                match++;
            } else {
                mismatch++;
            }
            arguments.add(new Argument(
                    propertyName,
                    parameter.getParameterizedType(),
                    value != null ? (InternalValue) value : (InternalValue) Values.NULL));
        }
        return new PropertiesMatch<>(match, mismatch, constructor, arguments);
    }

    private record PropertiesMatch<T>(int match, int mismatch, Constructor<T> constructor, List<Argument> arguments) {}
}
