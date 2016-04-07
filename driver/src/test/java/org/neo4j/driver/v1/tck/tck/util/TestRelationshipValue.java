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

import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.Value;

public class TestRelationshipValue extends RelationshipValue implements Entity
{
    public TestRelationshipValue( Relationship adapted )
    {
        super( adapted );
    }

    public TestRelationshipValue( int i, String type, Map<String,Value> properties )
    {
        super( new InternalRelationship( i, 0, 1, type, properties ) );
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
        RelationshipValue value = (RelationshipValue) o;
        return (value.asRelationship().type().equals( this.asRelationship().type() ) &&
                value.asMap().equals( this.asMap() ));
    }

    @Override
    public String toString()
    {
        return this.asRelationship().type() + this.asMap().toString();
    }

    @Override
    public long id()
    {
        return this.asRelationship().id();
    }
}