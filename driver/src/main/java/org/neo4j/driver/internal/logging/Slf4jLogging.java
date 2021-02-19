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
package org.neo4j.driver.internal.logging;

import org.slf4j.LoggerFactory;

import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;

/**
 * Internal implementation of the SLF4J logging.
 * <b>This class should not be used directly.</b> Please use {@link Logging#slf4j()} factory method instead.
 *
 * @see Logging#slf4j()
 */
public class Slf4jLogging implements Logging
{
    @Override
    public Logger getLog( String name )
    {
        return new Slf4jLogger( LoggerFactory.getLogger( name ) );
    }

    public static RuntimeException checkAvailability()
    {
        try
        {
            Class.forName( "org.slf4j.LoggerFactory" );
            return null;
        }
        catch ( Throwable error )
        {
            return new IllegalStateException(
                    "SLF4J logging is not available. Please add dependencies on slf4j-api and SLF4J binding (Logback, Log4j, etc.)",
                    error );
        }
    }
}
