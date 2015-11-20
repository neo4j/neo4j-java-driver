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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Identity;
import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Relationship;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.value.NotMultiValued;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.exceptions.value.Unsizable;
import org.neo4j.driver.v1.Function;

import static java.util.Collections.emptyList;

public abstract class ValueAdapter implements Value
{
    @Override
    public String javaString()
    {
        throw new Uncoercible( typeName(), "Java String" );
    }

    @Override
    public int javaInteger()
    {
        throw new Uncoercible( typeName(), "Java int" );
    }

    @Override
    public long javaLong()
    {
        throw new Uncoercible( typeName(), "Java long" );
    }

    @Override
    public float javaFloat()
    {
        throw new Uncoercible( typeName(), "Java float" );
    }

    @Override
    public double javaDouble()
    {
        throw new Uncoercible( typeName(), "Java double" );
    }

    @Override
    public boolean javaBoolean()
    {
        throw new Uncoercible( typeName(), "Java boolean" );
    }

    @Override
    public <T> List<T> javaList( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( typeName(), "Java List" );
    }

    @Override
    public <T> Map<String, T> javaMap( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( typeName(), "Java Map" );
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
    public Value get( long index )
    {
        throw new NotMultiValued( typeName() + " is not an indexed collection" );
    }

    @Override
    public Value get( String key )
    {
        throw new NotMultiValued( typeName() + " is not a keyed collection" );
    }

    @Override
    public long size()
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
    public Iterator<Value> iterator()
    {
        throw new NotMultiValued( typeName() + " is not iterable" );
    }

    @Override
    public String toString()
    {
        return String.format( "%s<>", typeName() );
    }

    protected String typeName()
    {
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
