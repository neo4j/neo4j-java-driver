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

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.value;

public class MapValueTest
{
    @Test
    public void shouldHaveSensibleToString() throws Throwable
    {
        MapValue mapValue = mapValue();
        assertThat( mapValue.toString(), equalTo( "{k1: \"v1\", k2: 42}" ) );
    }

    @Test
    public void shouldHaveCorrectPropertyCount() throws Throwable
    {
        MapValue mapValue = mapValue();
        assertThat( mapValue.size(), equalTo( 2 ) );
    }

    @Test
    public void shouldHaveCorrectType() throws Throwable
    {

        MapValue map = mapValue();

        assertThat(map.type(), equalTo( InternalTypeSystem.TYPE_SYSTEM.MAP() ));
    }

    @Test
    public void shouldNotBeNull() throws Throwable
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
