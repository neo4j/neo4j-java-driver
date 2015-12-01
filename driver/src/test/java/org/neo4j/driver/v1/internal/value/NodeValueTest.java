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

import java.util.HashMap;

import org.junit.Test;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.SimpleNode;
import org.neo4j.driver.v1.internal.types.TypeConstructor;

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class NodeValueTest
{
    @Test
    public void shouldHaveSensibleToString() throws Throwable
    {
        assertEquals("node<#1234>", nodeValue().toString());
    }

    private NodeValue nodeValue()
    {
        return new NodeValue( new SimpleNode( 1234, singletonList( "User" ), new HashMap<String, Value>() ) );
    }

    @Test
    public void shouldNotBeNull()
    {
        Value value = nodeValue();
        assertFalse( value.isNull() );
    }

    @Test
    public void shouldTypeAsNode()
    {
        InternalValue value = nodeValue();
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.NODE_TyCon ) );
    }
}
