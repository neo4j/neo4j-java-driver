/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.time.LocalTime;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LocalTimeValueTest
{
    @Test
    public void shouldHaveCorrectType()
    {
        LocalTime time = LocalTime.of( 23, 59, 59 );
        LocalTimeValue timeValue = new LocalTimeValue( time );
        assertEquals( InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME(), timeValue.type() );
    }

    @Test
    public void shouldSupportAsObject()
    {
        LocalTime time = LocalTime.of( 1, 17, 59, 999 );
        LocalTimeValue timeValue = new LocalTimeValue( time );
        assertEquals( time, timeValue.asObject() );
    }

    @Test
    public void shouldSupportAsLocalTime()
    {
        LocalTime time = LocalTime.of( 12, 59, 12, 999_999_999 );
        LocalTimeValue timeValue = new LocalTimeValue( time );
        assertEquals( time, timeValue.asLocalTime() );
    }

    @Test
    public void shouldNotSupportAsLong()
    {
        LocalTime time = LocalTime.now();
        LocalTimeValue timeValue = new LocalTimeValue( time );

        try
        {
            timeValue.asLong();
            fail( "Exception expected" );
        }
        catch ( Uncoercible ignore )
        {
        }
    }
}
