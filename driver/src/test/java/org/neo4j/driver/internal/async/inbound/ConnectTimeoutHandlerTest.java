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
package org.neo4j.driver.internal.async.inbound;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.neo4j.driver.exceptions.ServiceUnavailableException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectTimeoutHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFireExceptionOnTimeout() throws Exception
    {
        int timeoutMillis = 100;
        channel.pipeline().addLast( new ConnectTimeoutHandler( timeoutMillis ) );

        // sleep for more than the timeout value
        Thread.sleep( timeoutMillis * 4 );
        channel.runPendingTasks();

        ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, channel::checkException );
        assertEquals( error.getMessage(), "Unable to establish connection in " + timeoutMillis + "ms" );
    }

    @Test
    void shouldNotFireExceptionMultipleTimes() throws Exception
    {
        int timeoutMillis = 70;
        channel.pipeline().addLast( new ConnectTimeoutHandler( timeoutMillis ) );

        // sleep for more than the timeout value
        Thread.sleep( timeoutMillis * 4 );
        channel.runPendingTasks();

        ServiceUnavailableException error = assertThrows( ServiceUnavailableException.class, channel::checkException );
        assertEquals( error.getMessage(), "Unable to establish connection in " + timeoutMillis + "ms" );

        // sleep even more
        Thread.sleep( timeoutMillis * 4 );
        channel.runPendingTasks();

        // no more exceptions should occur
        channel.checkException();
    }
}
