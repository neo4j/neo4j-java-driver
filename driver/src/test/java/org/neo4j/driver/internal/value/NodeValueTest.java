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

import java.util.HashMap;

import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.v1.Value;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.value;

public class NodeValueTest
{
    @Test
    public void shouldHaveSensibleToString() throws Throwable
    {
        assertEquals( "node<1234>", emptyNodeValue().toString() );
        assertEquals( "node<1234>", filledNodeValue().toString() );
    }

    @Test
    public void shouldHaveCorrectPropertyCount() throws Throwable
    {
        assertEquals( 0, emptyNodeValue().size()) ;
        assertEquals( 1, filledNodeValue().size()) ;
    }

    @Test
    public void shouldNotBeNull()
    {
        assertFalse( emptyNodeValue().isNull() );
    }

    @Test
    public void shouldHaveCorrectType() throws Throwable
    {
        assertThat( emptyNodeValue().type(), equalTo( InternalTypeSystem.TYPE_SYSTEM.NODE() ));
    }

    @Test
    public void shouldTypeAsNode()
    {
        InternalValue value = emptyNodeValue();
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.NODE_TyCon ) );
    }

    private NodeValue emptyNodeValue()
    {
        return new NodeValue( new InternalNode( 1234, singletonList( "User" ), new HashMap<String, Value>() ) );
    }

    private NodeValue filledNodeValue()
    {
        return new NodeValue( new InternalNode( 1234, singletonList( "User" ), singletonMap( "name", value( "Dodo" ) ) ) );
    }
}
