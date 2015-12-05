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

import org.junit.Test;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.SimpleRelationship;
import org.neo4j.driver.v1.internal.types.TypeConstructor;

import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import static org.neo4j.driver.v1.Values.value;

public class RelationshipValueTest
{
    @Test
    public void shouldHaveSensibleToString() throws Throwable
    {
        assertEquals( "relationship<#1234> :: RELATIONSHIP", emptyRelationshipValue().toString() );
        assertEquals( "relationship<#1234> :: RELATIONSHIP", filledRelationshipValue().toString() );
    }

    @Test
    public void shouldHaveCorrectPropertyCount() throws Throwable
    {
        assertEquals( 0, emptyRelationshipValue().propertyCount()) ;
        assertEquals( 1, filledRelationshipValue().propertyCount()) ;
    }

    @Test
    public void shouldNotBeNull()
    {
        Value value = emptyRelationshipValue();
        assertFalse( value.isNull() );
    }

    @Test
    public void shouldTypeAsRelationship()
    {
        InternalValue value = emptyRelationshipValue();
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.RELATIONSHIP_TyCon ) );
    }

    private RelationshipValue emptyRelationshipValue()
    {
        return new RelationshipValue( new SimpleRelationship( 1234, 1, 2, "KNOWS" ) );
    }

    private RelationshipValue filledRelationshipValue()
    {
        return new RelationshipValue( new SimpleRelationship( 1234, 1, 2, "KNOWS", singletonMap( "name", value( "Dodo" ) ) ) );
    }
}
