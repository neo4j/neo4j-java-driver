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
package org.neo4j.driver.internal.async.pool;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PoolSettingsTest
{
    @Test
    void idleTimeBeforeConnectionTestWhenConfigured()
    {
        PoolSettings settings = new PoolSettings( 5, -1, 10, 42 );
        assertTrue( settings.idleTimeBeforeConnectionTestEnabled() );
        assertEquals( 42, settings.idleTimeBeforeConnectionTest() );
    }

    @Test
    void idleTimeBeforeConnectionTestWhenSetToZero()
    {
        //Always test idle time during acquisition
        PoolSettings settings = new PoolSettings( 5, -1, 10, 0 );
        assertTrue( settings.idleTimeBeforeConnectionTestEnabled() );
        assertEquals( 0, settings.idleTimeBeforeConnectionTest() );
    }

    @Test
    void idleTimeBeforeConnectionTestWhenSetToNegativeValue()
    {
        //Never test idle time during acquisition
        testIdleTimeBeforeConnectionTestWithIllegalValue( -1 );
        testIdleTimeBeforeConnectionTestWithIllegalValue( -42 );
        testIdleTimeBeforeConnectionTestWithIllegalValue( Integer.MIN_VALUE );
    }

    @Test
    void maxConnectionLifetimeWhenConfigured()
    {
        PoolSettings settings = new PoolSettings( 5, -1, 42, 10 );
        assertTrue( settings.maxConnectionLifetimeEnabled() );
        assertEquals( 42, settings.maxConnectionLifetime() );
    }

    @Test
    void maxConnectionLifetimeWhenSetToZeroOrNegativeValue()
    {
        testMaxConnectionLifetimeWithIllegalValue( 0 );
        testMaxConnectionLifetimeWithIllegalValue( -1 );
        testMaxConnectionLifetimeWithIllegalValue( -42 );
        testMaxConnectionLifetimeWithIllegalValue( Integer.MIN_VALUE );
    }

    private static void testIdleTimeBeforeConnectionTestWithIllegalValue( int value )
    {
        PoolSettings settings = new PoolSettings( 5, -1, 10, value );
        assertFalse( settings.idleTimeBeforeConnectionTestEnabled() );
    }

    private static void testMaxConnectionLifetimeWithIllegalValue( int value )
    {
        PoolSettings settings = new PoolSettings( 5, -1, value, 10 );
        assertFalse( settings.maxConnectionLifetimeEnabled() );
    }
}
