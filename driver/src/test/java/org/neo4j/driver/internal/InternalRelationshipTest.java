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
package org.neo4j.driver.internal;

import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.Values.NULL;
import static org.neo4j.driver.v1.Values.value;

public class InternalRelationshipTest
{
    @Test
    public void extractValuesFromNode()
    {
        // GIVEN
        InternalRelationship relationship = createRelationship();
        Function<Value,Integer> extractor = new Function<Value,Integer>()
        {
            @Override
            public Integer apply( Value value )
            {
                return value.asInt();
            }
        };

        //WHEN
        Iterable<Integer> values = relationship.values( extractor );

        //THEN
        Iterator<Integer> iterator = values.iterator();
        assertThat( iterator.next(), equalTo( 1 ) );
        assertThat( iterator.next(), equalTo( 2 ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void accessUnknownKeyShouldBeNull()
    {
        InternalRelationship relationship = createRelationship();

        assertThat( relationship.get( "k1" ), equalTo( value( 1 ) ) );
        assertThat( relationship.get( "k2" ), equalTo( value( 2 ) ) );
        assertThat( relationship.get( "k3" ), equalTo( NULL ) );
    }

    private InternalRelationship createRelationship()
    {
        Map<String,Value> props = new HashMap<>();
        props.put( "k1", value( 1 ) );
        props.put( "k2", value( 2 ) );

        return new InternalRelationship(1L, 0L, 1L, "T", props );
    }

}
