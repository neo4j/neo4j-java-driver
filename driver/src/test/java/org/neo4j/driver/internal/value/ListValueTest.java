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
import org.neo4j.driver.Value;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.neo4j.driver.Values.value;

class ListValueTest
{
    @Test
    void shouldHaveSensibleToString()
    {
        ListValue listValue = listValue( value( 1 ), value( 2 ), value( 3 ) );
        assertThat( listValue.toString(), equalTo( "[1, 2, 3]" ) );
    }

    @Test
    void shouldHaveCorrectType()
    {

        ListValue listValue = listValue();

        assertThat(listValue.type(), equalTo( InternalTypeSystem.TYPE_SYSTEM.LIST() ));
    }

    private ListValue listValue( Value... values )
    {
        return new ListValue( values );
    }
}
