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
package org.neo4j.driver.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.Value;

/**
 * {@link Node} implementation that directly contains labels and properties.
 */
public class InternalNode extends InternalEntity implements Node
{
    private final Collection<String> labels;

    public InternalNode( long id )
    {
        this( id, Collections.<String>emptyList(), Collections.<String,Value>emptyMap() );
    }

    public InternalNode( long id, Collection<String> labels, Map<String, Value> properties )
    {
        super( id, properties );
        this.labels = labels;
    }

    @Override
    public Collection<String> labels()
    {
        return labels;
    }

    @Override
    public boolean hasLabel( String label )
    {
        return labels.contains( label );
    }

    @Override
    public Value asValue()
    {
        return new NodeValue( this );
    }

    @Override
    public String toString()
    {
        return String.format( "node<%s>", id()  );
    }
}
