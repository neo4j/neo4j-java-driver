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

public final class Preconditions
{
    private Preconditions()
    {
    }

    /**
     * Assert that given expression is true.
     *
     * @param expression the value to check.
     * @param message the message.
     * @throws IllegalArgumentException if given value is {@code false}.
     */
    public static void checkArgument( boolean expression, String message )
    {
        if ( !expression )
        {
            throw new IllegalArgumentException( message );
        }
    }

    /**
     * Assert that given argument is of expected type.
     *
     * @param argument the object to check.
     * @param expectedClass the expected type.
     * @throws IllegalArgumentException if argument is not of expected type.
     */
    public static void checkArgument( Object argument, Class<?> expectedClass )
    {
        if ( !expectedClass.isInstance( argument ) )
        {
            throw new IllegalArgumentException( "Argument expected to be of type: " + expectedClass.getName() + " but was: " + argument );
        }
    }
}
