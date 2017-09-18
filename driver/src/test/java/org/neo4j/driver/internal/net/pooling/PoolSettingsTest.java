/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.net.pooling;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PoolSettingsTest
{
    @Test
    public void idleTimeBeforeConnectionTestWhenConfigured()
    {
        PoolSettings settings = new PoolSettings( 10, 42, 10, 5, -1 );
        assertTrue( settings.idleTimeBeforeConnectionTestEnabled() );
        assertEquals( 42, settings.idleTimeBeforeConnectionTest() );
    }

    @Test
    public void idleTimeBeforeConnectionTestWhenSetToZero()
    {
        //Always test idle time during acquisition
        PoolSettings settings = new PoolSettings( 10, 0, 10, 5, -1 );
        assertTrue( settings.idleTimeBeforeConnectionTestEnabled() );
        assertEquals( 0, settings.idleTimeBeforeConnectionTest() );
    }

    @Test
    public void idleTimeBeforeConnectionTestWhenSetToNegativeValue()
    {
        //Never test idle time during acquisition
        testIdleTimeBeforeConnectionTestWithIllegalValue( -1 );
        testIdleTimeBeforeConnectionTestWithIllegalValue( -42 );
        testIdleTimeBeforeConnectionTestWithIllegalValue( Integer.MIN_VALUE );
    }

    @Test
    public void maxConnectionLifetimeWhenConfigured()
    {
        PoolSettings settings = new PoolSettings( 10, 10, 42, 5, -1 );
        assertTrue( settings.maxConnectionLifetimeEnabled() );
        assertEquals( 42, settings.maxConnectionLifetime() );
    }

    @Test
    public void maxConnectionLifetimeWhenSetToZeroOrNegativeValue()
    {
        testMaxConnectionLifetimeWithIllegalValue( 0 );
        testMaxConnectionLifetimeWithIllegalValue( -1 );
        testMaxConnectionLifetimeWithIllegalValue( -42 );
        testMaxConnectionLifetimeWithIllegalValue( Integer.MIN_VALUE );
    }

    private static void testIdleTimeBeforeConnectionTestWithIllegalValue( int value )
    {
        PoolSettings settings = new PoolSettings( 10, value, 10, 5, -1 );
        assertFalse( settings.idleTimeBeforeConnectionTestEnabled() );
    }

    private static void testMaxConnectionLifetimeWithIllegalValue( int value )
    {
        PoolSettings settings = new PoolSettings( 10, 10, value, 5, -1 );
        assertFalse( settings.maxConnectionLifetimeEnabled() );
    }
}
