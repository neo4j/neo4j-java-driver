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
package org.neo4j.driver.v1.internal.value;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.Identity;
import org.neo4j.driver.v1.MapLike;
import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Relationship;
import org.neo4j.driver.v1.Type;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.value.NotMultiValued;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.exceptions.value.Unsizable;
import org.neo4j.driver.v1.internal.types.TypeConstructor;
import org.neo4j.driver.v1.internal.types.TypeRepresentation;
import org.neo4j.driver.v1.internal.util.Extract;

import static java.util.Collections.emptyList;

import static org.neo4j.driver.v1.Values.valueAsIs;

public abstract class ValueAdapter implements InternalValue
{
    @Override
    public boolean hasType( Type type )
    {
        return type.isTypeOf( this );
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    public boolean containsKey( String key )
    {
        throw new NotMultiValued( type().name() + " is not a keyed collection" );
    }

    @Override
    public String asString()
    {
        throw new Uncoercible( type().name(), "Java String" );
    }

    @Override
    public char asChar()
    {
        throw new Uncoercible( type().name(), "Java char" );
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
    public short asShort()
    {
        throw new Uncoercible( type().name(), "Java short" );
    }

    @Override
    public byte asByte()
    {
        throw new Uncoercible( type().name(), "Java byte" );
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
    public List<Value> asList()
    {
        return asList( valueAsIs() );
    }

    @Override
    public <T> List<T> asList( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( type().name(), "Java List" );
    }

    @Override
    public Map<String,Value> asMap()
    {
        return asMap( valueAsIs() );
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
    public Value[] asArray()
    {
        return asArray( Value.class, valueAsIs() );
    }

    @Override
    public <T> T[] asArray( Class<T> clazz, Function<Value, T> mapFunction )
    {
        throw new Uncoercible( type().name(), "Java T[]" );
    }

    @Override
    public char[] asCharArray()
    {
        throw new Uncoercible( type().name(), "Java char[]" );
    }

    @Override
    public long[] asLongArray()
    {
        throw new Uncoercible( type().name(), "Java long[]" );
    }

    @Override
    public int[] asIntArray()
    {
        throw new Uncoercible( type().name(), "Java int[]" );
    }

    @Override
    public short[] asShortArray()
    {
        throw new Uncoercible( type().name(), "Java short[]" );
    }

    @Override
    public byte[] asByteArray()
    {
        throw new Uncoercible( type().name(), "Java byte[]" );
    }

    @Override
    public double[] asDoubleArray()
    {
        return new double[0];
    }

    @Override
    public float[] asFloatArray()
    {
        return new float[0];
    }

    @Override
    public Number asNumber()
    {
        throw new Uncoercible( type().name(), "Java Number" );
    }

    @Override
    public Identity asIdentity()
    {
        throw new Uncoercible( type().name(), "Identity" );
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
    public Value value( int index )
    {
        throw new NotMultiValued( type().name() + " is not an indexed collection" );
    }

    @Override
    public Value value( String key )
    {
        throw new NotMultiValued( type().name() + " is not a keyed collection" );
    }

    @Override
    public int elementCount()
    {
        throw new Unsizable( type().name() + " does not have size" );
    }

    @Override
    public Iterable<String> keys()
    {
        return emptyList();
    }

    @Override
    public boolean hasElements()
    {
        return values().iterator().hasNext();
    }

    @Override
    public Iterable<Value> values()
    {
        return values( valueAsIs() );
    }

    @Override
    public <T> Iterable<T> values( Function<Value,T> mapFunction )
    {
        throw new NotMultiValued( type().name() + " is not iterable" );
    }

    @Override
    public Iterable<MapLike.Entry<Value>> entries()
    {
        return entries( valueAsIs() );
    }

    @Override
    public <V> Iterable<MapLike.Entry<V>> entries( final Function<Value, V> Function )
    {
        return Extract.entries( this, Function );
    }

    @Override
    public String toString()
    {
        return String.format( "%s<>", type().name() );
    }

    @Override
    public final TypeConstructor typeConstructor()
    {
        return ( (TypeRepresentation) type() ).constructor();
    }
}


