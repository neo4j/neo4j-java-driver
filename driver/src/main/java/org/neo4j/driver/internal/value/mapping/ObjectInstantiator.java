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
import org.neo4j.driver.exceptions.value.ValueException;

class ObjectInstantiator {

    <T> T instantiate(ObjectMetadata<T> metadata) {
        var constructor = metadata.constructor();
        var targetTypeName = constructor.getDeclaringClass().getCanonicalName();
        var initargs = initargs(targetTypeName, metadata.arguments());
        try {
            return constructor.newInstance(initargs);
        } catch (Throwable e) {
            throw new ValueException("Failed to instantiate '%s'".formatted(targetTypeName), e);
        }
    }

    private Object[] initargs(String targetTypeName, List<Argument> arguments) {
        var initargs = new Object[arguments.size()];
        for (var i = 0; i < initargs.length; i++) {
            var argument = arguments.get(i);
            var type = argument.type();
            try {
                initargs[i] = argument.value().as(type);
            } catch (Throwable e) {
                throw new ValueException(
                        "Failed to map '%s' property to '%s' for '%s' instantiation"
                                .formatted(argument.propertyName(), type.getTypeName(), targetTypeName),
                        e);
            }
        }
        return initargs;
    }
}
