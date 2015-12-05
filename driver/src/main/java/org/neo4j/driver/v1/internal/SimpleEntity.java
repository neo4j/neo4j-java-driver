/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1.internal;

import java.util.Map;

import org.neo4j.driver.v1.Entity;
import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Identity;
import org.neo4j.driver.v1.Property;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.internal.util.Extract;
import org.neo4j.driver.v1.internal.util.Iterables;
import org.neo4j.driver.v1.internal.value.MapValue;

import static org.neo4j.driver.v1.Values.valueAsIs;

public abstract class SimpleEntity implements Entity, AsValue
{
    private final Identity id;
    private final Map<String,Value> properties;

    public SimpleEntity( Identity id, Map<String,Value> properties )
    {
        this.id = id;
        this.properties = properties;
    }

    @Override
    public Identity identity()
    {
        return id;
    }

    @Override
    public Property property( String key )
    {
        return SimpleProperty.of( key, value( key ) );
    }

    public int propertyCount()
    {
        return properties.size();
    }

    public Value asValue()
    {
        return new MapValue( properties );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        SimpleEntity that = (SimpleEntity) o;

        return id.equals( that.id );

    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    @Override
    public String toString()
    {
        return "Entity{" +
               "id=" + id +
               ", properties=" + properties +
               '}';
    }

    @Override
    public boolean containsKey( String key )
    {
        return properties.containsKey( key );
    }

    @Override
    public Iterable<String> keys()
    {
        return properties.keySet();
    }

    @Override
    public Value value( String key )
    {
        Value value = properties.get( key );
        return value == null ? Values.NULL : value;
    }

    @Override
    public Iterable<Value> values()
    {
        return properties.values();
    }

    @Override
    public <T> Iterable<T> values( Function<Value,T> mapFunction )
    {
        return Iterables.map( properties.values(), mapFunction );
    }

    @Override
    public Iterable<Property<Value>> properties()
    {
        return properties( valueAsIs() );
    }

    @Override
    public <V> Iterable<Property<V>> properties( final Function<Value, V> Function )
    {
        return Extract.properties( this, Function );
    }
}
