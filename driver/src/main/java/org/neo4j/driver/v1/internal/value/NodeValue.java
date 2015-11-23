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

import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Value;

public class NodeValue extends ValueAdapter
{
    private final Node adapted;

    public NodeValue( Node adapted )
    {
        this.adapted = adapted;
    }

    @Override
    public Node asNode()
    {
        return adapted;
    }

    @Override
    public boolean isNode()
    {
        return true;
    }

    @Override
    public long size()
    {
        int count = 0;
        for ( String ignore : adapted.propertyKeys() ) { count++; }
        return count;
    }

    @Override
    public Iterable<String> keys()
    {
        return adapted.propertyKeys();
    }

    @Override
    public Value get( String key )
    {
        return adapted.property( key );
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

        NodeValue values = (NodeValue) o;

        return !(adapted != null ? !adapted.equals( values.adapted ) : values.adapted != null);

    }

    @Override
    public int hashCode()
    {
        return adapted != null ? adapted.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return adapted.toString();
    }
}
