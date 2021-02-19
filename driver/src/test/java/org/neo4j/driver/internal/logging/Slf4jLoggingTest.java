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

import org.junit.jupiter.api.Test;

import org.neo4j.driver.Logger;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

class Slf4jLoggingTest
{
    @Test
    void shouldCreateLoggers()
    {
        Slf4jLogging logging = new Slf4jLogging();

        Logger logger = logging.getLog( "My Log" );

        assertThat( logger, instanceOf( Slf4jLogger.class ) );
    }

    @Test
    void shouldCheckIfAvailable()
    {
        assertNull( Slf4jLogging.checkAvailability() );
    }
}
