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

import java.util.Collection;
import java.util.Map;

import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.Value;

public class TestNodeValue extends NodeValue implements Node
{
    private long id;

    public TestNodeValue( Node adapted )
    {
        super( adapted );
        id = adapted.id();
    }

    public TestNodeValue( long id, Collection<String> labels, Map<String,Value> properties )
    {
        super( new InternalNode( id, labels, properties ) );
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
        NodeValue value = (NodeValue) o;
        return (value.asNode().labels().equals( this.asNode().labels() ) && value.asMap()
                .equals( this.asMap() ));
    }

    @Override
    public String toString()
    {
        return this.asNode().labels() + this.asMap().toString();
    }

    @Override
    public long id()
    {
        return this.id;
    }

    @Override
    public Iterable<String> labels()
    {
        return this.asNode().labels();
    }

    @Override
    public boolean hasLabel( String label )
    {
        return this.asNode().hasLabel( label );
    }

    public void setId( long id )
    {
        this.id = id;
    }
}
