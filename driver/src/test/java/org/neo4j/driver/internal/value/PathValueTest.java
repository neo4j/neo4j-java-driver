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
package org.neo4j.driver.internal.value;

import org.junit.Test;

import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class PathValueTest
{
    @Test
    public void shouldHaveSensibleToString() throws Throwable
    {
        assertEquals("path[(42)-[43:T]->(44)]", pathValue().toString());
    }

    @Test
    public void shouldNotBeNull()
    {
        Value value = pathValue();
        assertFalse( value.isNull() );
    }


    @Test
    public void shouldHaveCorrectType() throws Throwable
    {

        assertThat(pathValue().type(), equalTo( InternalTypeSystem.TYPE_SYSTEM.PATH() ));
    }

    private PathValue pathValue()
    {
        return new PathValue( new InternalPath( new InternalNode(42L), new InternalRelationship( 43L, 42L, 44L, "T" ), new InternalNode( 44L ) ) );
    }
}
