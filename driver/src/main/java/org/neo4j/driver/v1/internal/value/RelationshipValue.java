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

import org.neo4j.driver.v1.Relationship;
import org.neo4j.driver.v1.Type;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.types.StandardTypeSystem;

public class RelationshipValue extends ValueAdapter
{
    private final Relationship adapted;

    public RelationshipValue( Relationship adapted )
    {
        this.adapted = adapted;
    }

    @Override
    public Object asObject()
    {
        return asRelationship();
    }

    @Override
    public Relationship asRelationship()
    {
        return adapted;
    }

    private boolean isRelationship()
    {
        return true;
    }

    @Override
    public int elementCount()
    {
        int count = 0;
        for ( String ignore : adapted.keys() ) { count++; }
        return count;
    }

    @Override
    public Iterable<String> keys()
    {
        return adapted.keys();
    }

    public Value value( String key )
    {
        return adapted.value( key );
    }

    @Override
    public Type type()
    {
        return StandardTypeSystem.TYPE_SYSTEM.RELATIONSHIP();
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

        RelationshipValue values = (RelationshipValue) o;
        return adapted == values.adapted || adapted.equals( values.adapted );
    }

    @Override
    public int hashCode()
    {
        return adapted == null ? 0 : adapted.hashCode();
    }

    @Override
    public String toString()
    {
        return adapted.toString();
    }
}
