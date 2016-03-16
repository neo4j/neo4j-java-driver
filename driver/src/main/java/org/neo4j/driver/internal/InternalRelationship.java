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

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.Value;

/**
 * {@link Relationship} implementation that directly contains type and properties.
 */
public class InternalRelationship extends InternalEntity implements Relationship
{
    private long start;
    private long end;
    private final String type;

    public InternalRelationship( long id, long start, long end, String type )
    {
        this( id, start, end, type, Collections.<String,Value>emptyMap() );
    }

    public InternalRelationship( long id, long start, long end, String type,
                                 Map<String, Value> properties )
    {
        super( id, properties );
        this.start = start;
        this.end = end;
        this.type = type;
    }

    @Override
    public boolean hasType( String relationshipType )
    {
        return type().equals( relationshipType );
    }

    /** Modify the start/end identities of this relationship */
    public void setStartAndEnd( long start, long end )
    {
        this.start = start;
        this.end = end;
    }

    @Override
    public long startNodeId()
    {
        return start;
    }

    @Override
    public long endNodeId()
    {
        return end;
    }

    @Override
    public String type()
    {
        return type;
    }

    @Override
    public Value asValue()
    {
        return new RelationshipValue( this );
    }

    @Override
    public String toString()
    {
        return String.format( "relationship<%s>", id() );
    }
}
