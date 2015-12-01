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
import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Relationship;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.value.NotMultiValued;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.exceptions.value.Unsizable;

import static java.util.Collections.emptyList;

import static org.neo4j.driver.v1.Values.valueAsIs;

public abstract class ValueAdapter implements InternalValue
{
    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public String asString()
    {
        throw new Uncoercible( typeName(), "Java String" );
    }

    @Override
    public char asChar()
    {
        throw new Uncoercible( typeName(), "Java char" );
    }

    @Override
    public long asLong()
    {
        throw new Uncoercible( typeName(), "Java long" );
    }

    @Override
    public int asInt()
    {
        throw new Uncoercible( typeName(), "Java int" );
    }

    @Override
    public short asShort()
    {
        throw new Uncoercible( typeName(), "Java short" );
    }

    @Override
    public byte asByte()
    {
        throw new Uncoercible( typeName(), "Java byte" );
    }

    @Override
    public float asFloat()
    {
        throw new Uncoercible( typeName(), "Java float" );
    }

    @Override
    public double asDouble()
    {
        throw new Uncoercible( typeName(), "Java double" );
    }

    @Override
    public boolean asBoolean()
    {
        throw new Uncoercible( typeName(), "Java boolean" );
    }

    @Override
    public <T> List<T> asList( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( typeName(), "Java List" );
    }

    @Override
    public <T> Map<String, T> asMap( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( typeName(), "Java Map" );
    }

    @Override
    public Object asObject()
    {
        throw new Uncoercible( typeName(), "Java Object" );
    }

    @Override
    public Value[] asArray()
    {
        return asArray( Value.class, valueAsIs() );
    }

    @Override
    public <T> T[] asArray( Class<T> clazz, Function<Value, T> mapFunction )
    {
        throw new Uncoercible( typeName(), "Java T[]" );
    }

    @Override
    public char[] asCharArray()
    {
        throw new Uncoercible( typeName(), "Java char[]" );
    }

    @Override
    public long[] asLongArray()
    {
        throw new Uncoercible( typeName(), "Java long[]" );
    }

    @Override
    public int[] asIntArray()
    {
        throw new Uncoercible( typeName(), "Java int[]" );
    }

    @Override
    public short[] asShortArray()
    {
        throw new Uncoercible( typeName(), "Java short[]" );
    }

    @Override
    public byte[] asByteArray()
    {
        throw new Uncoercible( typeName(), "Java byte[]" );
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
        throw new Uncoercible( typeName(), "Java Number" );
    }

    @Override
    public Identity asIdentity()
    {
        throw new Uncoercible( typeName(), "Identity" );
    }

    @Override
    public Node asNode()
    {
        throw new Uncoercible( typeName(), "Node" );
    }

    @Override
    public Path asPath()
    {
        throw new Uncoercible( typeName(), "Path" );
    }

    @Override
    public Relationship asRelationship()
    {
        throw new Uncoercible( typeName(), "Relationship" );
    }

    @Override
    public Value value( int index )
    {
        throw new NotMultiValued( typeName() + " is not an indexed collection" );
    }

    @Override
    public Value value( String key )
    {
        throw new NotMultiValued( typeName() + " is not a keyed collection" );
    }

    @Override
    public int elementCount()
    {
        throw new Unsizable( typeName() + " does not have size" );
    }

    @Override
    public Iterable<String> keys()
    {
        return emptyList();
    }

    @Override
    public boolean isString()
    {
        return false;
    }

    @Override
    public boolean isInteger()
    {
        return false;
    }

    @Override
    public boolean isFloat()
    {
        return false;
    }

    @Override
    public boolean isBoolean()
    {
        return false;
    }

    @Override
    public boolean isIdentity()
    {
        return false;
    }

    @Override
    public boolean isNode()
    {
        return false;
    }

    @Override
    public boolean isPath()
    {
        return false;
    }

    @Override
    public boolean isRelationship()
    {
        return false;
    }

    @Override
    public boolean isList()
    {
        return false;
    }

    @Override
    public boolean isMap()
    {
        return false;
    }

    @Override
    public boolean hasElements()
    {
        return values().iterator().hasNext();
    }

    @Override
    public <T> Iterable<T> values( Function<Value,T> mapFunction )
    {

        throw new NotMultiValued( typeName() + " is not iterable" );
    }

    @Override
    public List<Value> asList()
    {
        return asList( valueAsIs() );
    }

    @Override
    public Iterable<Value> values()
    {
        return values( valueAsIs() );
    }

    @Override
    public Map<String,Value> asMap()
    {
        return asMap( valueAsIs() );
    }

    @Override
    public String toString()
    {
        return String.format( "%s<>", typeName() );
    }

    protected String typeName()
    {
        if ( isNull() ) { return "null"; }
        if ( isFloat() ) { return "float"; }
        if ( isInteger() ) { return "integer"; }
        if ( isBoolean() ) { return "boolean"; }
        if ( isString() ) { return "string"; }
        if ( isList() ) { return "list"; }
        if ( isMap() ) { return "map"; }
        if ( isIdentity() ) { return "identity"; }
        if ( isNode() ) { return "node"; }
        if ( isRelationship() ) { return "relationship"; }
        if ( isPath() ) { return "path"; }
        return "unknown";
    }
}


