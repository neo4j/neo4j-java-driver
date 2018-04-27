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
package org.neo4j.driver.internal.net;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.Function;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith( Parameterized.class )
public class ConcurrencyGuardingConnectionTest
{
    @Parameterized.Parameter
    public Function<Connection, Void> operation;

    @Parameterized.Parameters
    public static List<Function<Connection,Void>> params()
    {
        return asList(
                INIT,
                RUN,
                PULL_ALL,
                DISCARD_ALL,
                RECIEVE_ONE,
                FLUSH,
                SYNC,
                RESET,
                ACK_FAILURE
        );
    }

    @Test
    public void shouldNotAllowConcurrentAccess() throws Throwable
    {
        // Given
        final AtomicReference<Connection> conn = new AtomicReference<>();
        final AtomicReference<ClientException> exception = new AtomicReference<>();

        Connection delegate = mock( Connection.class, new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                try
                {
                    operation.apply( conn.get() );
                    fail("Expected this call to fail, because it is calling a method on the connector while 'inside' " +
                         "a connector call already.");
                } catch(ClientException e)
                {
                    exception.set( e );
                }
                return null;
            }
        });

        conn.set(new ConcurrencyGuardingConnection( delegate ));

        // When
        operation.apply( conn.get() );

        // Then
        assertThat( exception.get().getMessage(), equalTo(
                "You are using a session from multiple locations at the same time, " +
                "which is not supported. If you want to use multiple threads, you should ensure " +
                "that each session is used by only one thread at a time. One way to " +
                "do that is to give each thread its own dedicated session.") );
    }

    @Test
    public void shouldAllowConcurrentClose()
    {
        // Given
        final AtomicReference<Connection> connection = new AtomicReference<>();

        Connection delegate = mock( Connection.class, new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                connection.get().close();
                return null;
            }
        } );
        doNothing().when( delegate ).close();

        connection.set( new ConcurrencyGuardingConnection( delegate ) );

        // When
        operation.apply( connection.get() );

        // Then
        verify( delegate ).close();
    }

    public static final Function<Connection,Void> INIT = new Function<Connection,Void>()
    {
        @Override
        public Void apply( Connection connection )
        {
            connection.init(null, null);
            return null;
        }
    };

    public static final Function<Connection,Void> RUN = new Function<Connection,Void>()
    {
        @Override
        public Void apply( Connection connection )
        {
            connection.run(null, null, null);
            return null;
        }
    };

    public static final Function<Connection,Void> DISCARD_ALL = new Function<Connection,Void>()
    {
        @Override
        public Void apply( Connection connection )
        {
            connection.discardAll(null);
            return null;
        }
    };

    public static final Function<Connection,Void> PULL_ALL = new Function<Connection,Void>()
    {
        @Override
        public Void apply( Connection connection )
        {
            connection.pullAll(null);
            return null;
        }
    };

    public static final Function<Connection,Void> RESET = new Function<Connection,Void>()
    {
        @Override
        public Void apply( Connection connection )
        {
            connection.reset();
            return null;
        }
    };

    public static final Function<Connection,Void> ACK_FAILURE = new Function<Connection,Void>()
    {
        @Override
        public Void apply( Connection connection )
        {
            connection.ackFailure();
            return null;
        }
    };

    public static final Function<Connection,Void> RECIEVE_ONE = new Function<Connection,Void>()
    {
        @Override
        public Void apply( Connection connection )
        {
            connection.receiveOne();
            return null;
        }
    };

    public static final Function<Connection,Void> SYNC = new Function<Connection,Void>()
    {
        @Override
        public Void apply( Connection connection )
        {
            connection.sync();
            return null;
        }
    };

    public static final Function<Connection,Void> FLUSH = new Function<Connection,Void>()
    {
        @Override
        public Void apply( Connection connection )
        {
            connection.flush();
            return null;
        }
    };
}
