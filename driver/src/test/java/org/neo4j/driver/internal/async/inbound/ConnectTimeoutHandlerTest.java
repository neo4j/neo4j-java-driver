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
package org.neo4j.driver.internal.async.inbound;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ConnectTimeoutHandlerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @After
    public void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldFireExceptionOnTimeout() throws Exception
    {
        int timeoutMillis = 100;
        channel.pipeline().addLast( new ConnectTimeoutHandler( timeoutMillis ) );

        // sleep for more than the timeout value
        Thread.sleep( timeoutMillis * 4 );
        channel.runPendingTasks();

        try
        {
            channel.checkException();
            fail( "Exception expected" );
        }
        catch ( ServiceUnavailableException e )
        {
            assertEquals( e.getMessage(), "Unable to establish connection in " + timeoutMillis + "ms" );
        }
    }

    @Test
    public void shouldNotFireExceptionMultipleTimes() throws Exception
    {
        int timeoutMillis = 70;
        channel.pipeline().addLast( new ConnectTimeoutHandler( timeoutMillis ) );

        // sleep for more than the timeout value
        Thread.sleep( timeoutMillis * 4 );
        channel.runPendingTasks();

        try
        {
            channel.checkException();
            fail( "Exception expected" );
        }
        catch ( ServiceUnavailableException e )
        {
            assertEquals( e.getMessage(), "Unable to establish connection in " + timeoutMillis + "ms" );
        }

        // sleep even more
        Thread.sleep( timeoutMillis * 4 );
        channel.runPendingTasks();

        // no more exceptions should occur
        channel.checkException();
    }
}
