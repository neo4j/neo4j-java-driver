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

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Type;

public class PathValue extends ObjectValueAdapter<Path> {
    public PathValue(Path adapted) {
        super(adapted);
    }

    @Override
    public Path asPath() {
        return asObject();
    }

    @Override
    public int size() {
        return asObject().length();
    }

    @Override
    public Type type() {
        return InternalTypeSystem.TYPE_SYSTEM.PATH();
    }

    @Override
    public BoltValue asBoltValue() {
        return new BoltValue(this, org.neo4j.bolt.connection.values.Type.PATH);
    }

    @Override
    public <T> T as(Class<T> targetClass) {
        if (targetClass.isAssignableFrom(Path.class)) {
            return targetClass.cast(asPath());
        }
        return asMapped(targetClass);
    }
}
