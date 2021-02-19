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

import org.neo4j.driver.Value;
import org.neo4j.driver.util.Pair;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.neo4j.driver.Values.value;

class InternalPairTest
{
    @Test
    void testMethods()
    {
        Pair<String, Value> pair = InternalPair.of( "k", value( "v" ) );
        assertThat( pair.key(), equalTo("k"));
        assertThat( pair.value(), equalTo(value("v")));
    }
}
