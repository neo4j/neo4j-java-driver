/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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

import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.types.Entity;
import java.util.function.Function;

public abstract class EntityValueAdapter<V extends Entity> extends ObjectValueAdapter<V>
{
    protected EntityValueAdapter( V adapted )
    {
        super( adapted );
    }

    @Override
    public V asEntity()
    {
        return asObject();
    }

    @Override
    public Map<String,Object> asMap()
    {
        return asEntity().asMap();
    }

    @Override
    public <T> Map<String,T> asMap( Function<Value,T> mapFunction )
    {
        return asEntity().asMap( mapFunction );
    }

    @Override
    public int size()
    {
        return asEntity().size();
    }

    @Override
    public Iterable<String> keys()
    {
        return asEntity().keys();
    }

    @Override
    public Value get( String key )
    {
        return asEntity().get( key );
    }
}
