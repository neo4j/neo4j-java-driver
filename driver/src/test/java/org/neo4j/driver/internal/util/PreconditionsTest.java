/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal.util;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.internal.util.Preconditions.checkArgument;

class PreconditionsTest
{
    @Test
    void shouldCheckBooleanArgument()
    {
        assertDoesNotThrow( () -> checkArgument( true, "" ) );
        assertDoesNotThrow( () -> checkArgument( !Duration.ofSeconds( 1 ).isZero(), "" ) );

        assertThrows( IllegalArgumentException.class, () -> checkArgument( false, "" ) );
        assertThrows( IllegalArgumentException.class, () -> checkArgument( Period.ofDays( 2 ).isNegative(), "" ) );
    }

    @Test
    void shouldCheckArgumentType()
    {
        assertDoesNotThrow( () -> checkArgument( "Hello", String.class ) );
        assertDoesNotThrow( () -> checkArgument( new ArrayList<>(), List.class ) );

        assertThrows( IllegalArgumentException.class, () -> checkArgument( 42, String.class ) );
        assertThrows( IllegalArgumentException.class, () -> checkArgument( new ArrayList<>(), Map.class ) );
    }
}
