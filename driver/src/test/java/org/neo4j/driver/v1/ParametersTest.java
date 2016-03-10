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
package org.neo4j.driver.v1;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.v1.exceptions.ClientException;

public class ParametersTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldGiveHelpfulMessageOnMisalignedInput() throws Throwable
    {
        // Expect
        exception.expect( ClientException.class );
        exception.expectMessage( "Parameters function requires an even number of arguments, " +
                                 "alternating key and value." );

        // When
        Values.parameters( "1", new Object(), "2" );
    }
}
