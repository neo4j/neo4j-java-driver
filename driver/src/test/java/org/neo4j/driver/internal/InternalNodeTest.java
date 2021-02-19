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
package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.driver.Value;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.driver.Values.NULL;
import static org.neo4j.driver.Values.value;

class InternalNodeTest
{
    @Test
    void extractValuesFromNode()
    {
        // GIVEN
        InternalNode node = createNode();
        Function<Value,Integer> extractor = Value::asInt;

        //WHEN
        Iterable<Integer> values = node.values( extractor );

        //THEN
        Iterator<Integer> iterator = values.iterator();
        assertThat( iterator.next(), equalTo( 1 ) );
        assertThat( iterator.next(), equalTo( 2 ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    void accessUnknownKeyShouldBeNull()
    {
        InternalNode node = createNode();

        assertThat( node.get( "k1" ), equalTo( value( 1 ) ) );
        assertThat( node.get( "k2" ), equalTo( value( 2 ) ) );
        assertThat( node.get( "k3" ), equalTo( NULL ) );
    }

    private InternalNode createNode()
    {
        Map<String,Value> props = new HashMap<>();
        props.put( "k1", value( 1 ) );
        props.put( "k2", value( 2 ) );
        return new InternalNode( 42L, Collections.singletonList( "L" ), props );
    }

}
