/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.value;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.types.InternalMapAccessorWithDefaultValue;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.value.NotMultiValued;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.exceptions.value.Unsizable;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.types.Type;
import org.neo4j.driver.v1.util.Function;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.driver.internal.value.InternalValue.Format.VALUE_ONLY;
import static org.neo4j.driver.v1.Values.ofValue;
import static org.neo4j.driver.v1.Values.ofObject;

public abstract class ValueAdapter extends InternalMapAccessorWithDefaultValue implements InternalValue
{
    @Override
    public Value asValue()
    {
        return this;
    }

    @Override
    public boolean hasType( Type type )
    {
        return type.isTypeOf( this );
    }

    @Override
    public boolean isTrue()
    {
        return false;
    }

    @Override
    public boolean isFalse()
    {
        return false;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public boolean containsKey( String key )
    {
        throw new NotMultiValued( type().name() + " is not a keyed collection" );
    }

    @Override
    public String asString()
    {
        throw new Uncoercible( type().name(), "Java String" );
    }

    public String asLiteralString()
    {
        throw new Uncoercible( type().name(), "Java String representation of Cypher literal" );
    }

    @Override
    public long asLong()
    {
        throw new Uncoercible( type().name(), "Java long" );
    }

    @Override
    public int asInt()
    {
        throw new Uncoercible( type().name(), "Java int" );
    }

    @Override
    public float asFloat()
    {
        throw new Uncoercible( type().name(), "Java float" );
    }

    @Override
    public double asDouble()
    {
        throw new Uncoercible( type().name(), "Java double" );
    }

    @Override
    public boolean asBoolean()
    {
        throw new Uncoercible( type().name(), "Java boolean" );
    }

    @Override
    public List<Object> asList()
    {
        return asList( ofObject() );
    }

    @Override
    public <T> List<T> asList( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( type().name(), "Java List" );
    }

    @Override
    public Map<String,Object> asMap()
    {
        return asMap( ofObject() );
    }

    @Override
    public <T> Map<String, T> asMap( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( type().name(), "Java Map" );
    }

    @Override
    public Object asObject()
    {
        throw new Uncoercible( type().name(), "Java Object" );
    }

    @Override
    public Number asNumber()
    {
        throw new Uncoercible( type().name(), "Java Number" );
    }

    @Override
    public Entity asEntity()
    {
        throw new Uncoercible( type().name(), "Entity" );
    }

    @Override
    public Node asNode()
    {
        throw new Uncoercible( type().name(), "Node" );
    }

    @Override
    public Path asPath()
    {
        throw new Uncoercible( type().name(), "Path" );
    }

    @Override
    public Relationship asRelationship()
    {
        throw new Uncoercible( type().name(), "Relationship" );
    }

    @Override
    public Value get( int index )
    {
        throw new NotMultiValued( type().name() + " is not an indexed collection" );
    }

    @Override
    public Value get( String key )
    {
        throw new NotMultiValued( type().name() + " is not a keyed collection" );
    }

    @Override
    public int size()
    {
        throw new Unsizable( type().name() + " does not have size" );
    }

    @Override
    public Iterable<String> keys()
    {
        return emptyList();
    }

    @Override
    public boolean isEmpty()
    {
        return ! values().iterator().hasNext();
    }

    @Override
    public Iterable<Value> values()
    {
        return values( ofValue() );
    }

    @Override
    public <T> Iterable<T> values( Function<Value,T> mapFunction )
    {
        throw new NotMultiValued( type().name() + " is not iterable" );
    }

    public String toString()
    {
        return toString( VALUE_ONLY );
    }

    protected String maybeWithType( boolean includeType, String text )
    {
        return includeType ? withType( text ) : text;
    }

    private String withType( String text )
    {
        return format( "%s :: %s", text, type().name() );
    }

    @Override
    public final TypeConstructor typeConstructor()
    {
        return ( (TypeRepresentation) type() ).constructor();
    }
}


