/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.v1.types;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.v1.Value;

/**
 * {@link Relationship} implementation that directly contains type and properties.
 */
public class SelfContainedRelationship extends SelfContainedEntity implements Relationship
{
    protected long startNodeId;
    protected long endNodeId;
    protected final String type;

    public SelfContainedRelationship(long id, long startNodeId, long endNodeId, String type )
    {
        this( id, startNodeId, endNodeId, type, Collections.<String,Value>emptyMap() );
    }

    public SelfContainedRelationship(long id, long startNodeId, long endNodeId, String type,
                                     Map<String, Value> properties )
    {
        super( id, properties );
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.type = type;
    }

    public SelfContainedRelationship( SelfContainedRelationship relationship, long startNodeId, long endNodeId )
    {
        this( relationship.id, startNodeId, endNodeId, relationship.type, relationship.properties );
    }

    @Override
    public boolean hasType( String relationshipType )
    {
        return type().equals( relationshipType );
    }

    @Override
    public long startNodeId()
    {
        return startNodeId;
    }

    @Override
    public long endNodeId()
    {
        return endNodeId;
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
