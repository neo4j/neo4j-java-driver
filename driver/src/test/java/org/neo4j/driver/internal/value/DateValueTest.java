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
package org.neo4j.driver.internal.value;

import org.junit.Test;

import java.time.LocalDate;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DateValueTest
{
    @Test
    public void shouldHaveCorrectType()
    {
        LocalDate localDate = LocalDate.now();
        DateValue dateValue = new DateValue( localDate );
        assertEquals( InternalTypeSystem.TYPE_SYSTEM.DATE(), dateValue.type() );
    }

    @Test
    public void shouldSupportAsObject()
    {
        LocalDate localDate = LocalDate.now();
        DateValue dateValue = new DateValue( localDate );
        assertEquals( localDate, dateValue.asObject() );
    }

    @Test
    public void shouldSupportAsLocalDate()
    {
        LocalDate localDate = LocalDate.now();
        DateValue dateValue = new DateValue( localDate );
        assertEquals( localDate, dateValue.asLocalDate() );
    }

    @Test
    public void shouldNotSupportAsLong()
    {
        LocalDate localDate = LocalDate.now();
        DateValue dateValue = new DateValue( localDate );

        try
        {
            dateValue.asLong();
            fail( "Exception expected" );
        }
        catch ( Uncoercible ignore )
        {
        }
    }
}
