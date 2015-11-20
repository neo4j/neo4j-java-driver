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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.v1.Identity;
import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Value;

/**
 * {@link Node} implementation that directly contains labels and properties.
 */
public class SimpleNode extends SimpleEntity implements Node
{
    private final Collection<String> labels;

    public SimpleNode( long id )
    {
        this( id, Collections.<String>emptyList(), Collections.<String,Value>emptyMap() );
    }

    public SimpleNode( long id, Collection<String> labels, Map<String,Value> properties )
    {
        this( Identities.identity( id ), labels, properties );
    }

    public SimpleNode( Identity identity, Collection<String> labels, Map<String,Value> properties )
    {
        super( identity, properties );
        this.labels = labels;
    }

    @Override
    public Collection<String> labels()
    {
        return labels;
    }

    @Override
    public String toString()
    {
        return "node<" + identity() + '>';
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
        if ( !super.equals( o ) )
        {
            return false;
        }

        SimpleNode that = (SimpleNode) o;

        return !(labels != null ? !labels.equals( that.labels ) : that.labels != null);

    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (labels != null ? labels.hashCode() : 0);
        return result;
    }
}
