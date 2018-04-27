/*
 * Copyright (c) 2002-2018 Neo4j Sweden AB [http://neo4j.com]
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

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PoolSettingsTest
{
    @Test
    public void idleTimeBeforeConnectionTestWhenConfigured()
    {
        PoolSettings settings = new PoolSettings( 10, 42 );
        assertTrue( settings.idleTimeBeforeConnectionTestConfigured() );
        assertEquals( 42, settings.idleTimeBeforeConnectionTest() );
    }

    @Test
    public void idleTimeBeforeConnectionTestWhenSetToZero()
    {
        PoolSettings settings = new PoolSettings( 10, 0 );
        assertTrue( settings.idleTimeBeforeConnectionTestConfigured() );
        assertEquals( 0, settings.idleTimeBeforeConnectionTest() );
    }

    @Test
    public void idleTimeBeforeConnectionTestWhenSetToNegativeValue()
    {
        testWithIllegalValue( -1 );
        testWithIllegalValue( -42 );
        testWithIllegalValue( Integer.MIN_VALUE );
    }

    private static void testWithIllegalValue( int value )
    {
        PoolSettings settings = new PoolSettings( 10, value );

        assertFalse( settings.idleTimeBeforeConnectionTestConfigured() );

        try
        {
            settings.idleTimeBeforeConnectionTest();
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }
}
