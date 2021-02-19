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

import java.util.HashMap;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.Value;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.driver.Values.value;

class MapValueTest
{
    @Test
    void shouldHaveSensibleToString()
    {
        MapValue mapValue = mapValue();
        assertThat( mapValue.toString(), equalTo( "{k1: \"v1\", k2: 42}" ) );
    }

    @Test
    void shouldHaveCorrectPropertyCount()
    {
        MapValue mapValue = mapValue();
        assertThat( mapValue.size(), equalTo( 2 ) );
    }

    @Test
    void shouldHaveCorrectType()
    {

        MapValue map = mapValue();

        assertThat(map.type(), equalTo( InternalTypeSystem.TYPE_SYSTEM.MAP() ));
    }

    @Test
    void shouldNotBeNull()
    {
        MapValue map = mapValue();

       assertFalse(map.isNull());
    }

    private MapValue mapValue()
    {
        HashMap<String,Value> map =  new HashMap<>();
        map.put( "k1", value( "v1" ) );
        map.put( "k2", value( 42 ) );
        return new MapValue( map );
    }
}
