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
package org.neo4j.driver.v1.tck.tck.util;

import java.util.Map;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.util.Function;

public class TestRelationship implements Relationship
{
    private final TestRelationshipValue trv;
    private final TestNodeValue start, end;

    public TestRelationship( TestRelationshipValue trv, TestNodeValue start, TestNodeValue end )
    {
        this.trv = trv;
        this.start = start;
        this.end = end;
    }

    @Override
    public String type()
    {
        return trv.asRelationship().type();
    }

    @Override
    public boolean hasType( String relationshipType )
    {
        return trv.asRelationship().hasType( relationshipType );
    }

    @Override
    public long startNodeId()
    {
        return this.start.id();
    }

    @Override
    public long endNodeId()
    {
        return this.end.id();
    }

    @Override
    public long id()
    {
        return trv.id();
    }

    @Override
    public Iterable<String> keys()
    {
        return trv.keys();
    }

    @Override
    public boolean containsKey( String key )
    {
        return trv.containsKey( key );
    }

    @Override
    public Value get( String key )
    {
        return trv.get( key );
    }

    @Override
    public int size()
    {
        return trv.size();
    }

    @Override
    public Iterable<Value> values()
    {
        return trv.values();
    }

    @Override
    public <T> Iterable<T> values( Function<Value,T> mapFunction )
    {
        return trv.values( mapFunction );
    }

    @Override
    public Map<String,Object> asMap()
    {
        return trv.asMap();
    }

    @Override
    public <T> Map<String,T> asMap( Function<Value,T> mapFunction )
    {
        return trv.asMap( mapFunction );
    }

    public TestRelationshipValue getTestRelationshipValue()
    {
        return trv;
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
        TestRelationship tr = (TestRelationship) o;
        return (this.getTestRelationshipValue().equals( tr.getTestRelationshipValue() ) && this.start.equals( tr.start ) && this
                .end.equals( tr.end ));
    }
}