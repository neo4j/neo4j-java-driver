/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.driver.internal.util.ValueFactory.emptyNodeValue;
import static org.neo4j.driver.internal.util.ValueFactory.filledNodeValue;

class NodeValueTest
{
    @Test
    void shouldHaveSensibleToString()
    {
        assertEquals( "node<1234>", emptyNodeValue().toString() );
        assertEquals( "node<1234>", filledNodeValue().toString() );
    }

    @Test
    void shouldHaveCorrectPropertyCount()
    {
        assertEquals( 0, emptyNodeValue().size()) ;
        assertEquals( 1, filledNodeValue().size()) ;
    }

    @Test
    void shouldNotBeNull()
    {
        assertFalse( emptyNodeValue().isNull() );
    }

    @Test
    void shouldHaveCorrectType()
    {
        assertThat( emptyNodeValue().type(), equalTo( InternalTypeSystem.TYPE_SYSTEM.NODE() ));
    }

    @Test
    void shouldTypeAsNode()
    {
        InternalValue value = emptyNodeValue();
        assertThat( value.typeConstructor(), equalTo( TypeConstructor.NODE ) );
    }
}
