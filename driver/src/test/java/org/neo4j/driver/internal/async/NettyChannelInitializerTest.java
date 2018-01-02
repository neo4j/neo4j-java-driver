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
package org.neo4j.driver.internal.async;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import org.junit.After;
import org.junit.Test;

import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.FakeClock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.async.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.async.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.async.ChannelAttributes.serverAddress;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

public class NettyChannelInitializerTest
{
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @After
    public void tearDown()
    {
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldAddSslHandlerWhenRequiresEncryption() throws Exception
    {
        SecurityPlan security = SecurityPlan.forAllCertificates();
        NettyChannelInitializer initializer = newInitializer( security );

        initializer.initChannel( channel );

        assertNotNull( channel.pipeline().get( SslHandler.class ) );
    }

    @Test
    public void shouldNotAddSslHandlerWhenDoesNotRequireEncryption()
    {
        SecurityPlan security = SecurityPlan.insecure();
        NettyChannelInitializer initializer = newInitializer( security );

        initializer.initChannel( channel );

        assertNull( channel.pipeline().get( SslHandler.class ) );
    }

    @Test
    public void shouldAddSslHandlerWithHandshakeTimeout() throws Exception
    {
        int timeoutMillis = 424242;
        SecurityPlan security = SecurityPlan.forAllCertificates();
        NettyChannelInitializer initializer = newInitializer( security, timeoutMillis );

        initializer.initChannel( channel );

        SslHandler sslHandler = channel.pipeline().get( SslHandler.class );
        assertNotNull( sslHandler );
        assertEquals( timeoutMillis, sslHandler.getHandshakeTimeoutMillis() );
    }

    @Test
    public void shouldUpdateChannelAttributes()
    {
        Clock clock = mock( Clock.class );
        when( clock.millis() ).thenReturn( 42L );
        SecurityPlan security = SecurityPlan.insecure();
        NettyChannelInitializer initializer = newInitializer( security, Integer.MAX_VALUE, clock );

        initializer.initChannel( channel );

        assertEquals( LOCAL_DEFAULT, serverAddress( channel ) );
        assertEquals( 42L, creationTimestamp( channel ) );
        assertNotNull( messageDispatcher( channel ) );
    }

    private static NettyChannelInitializer newInitializer( SecurityPlan securityPlan )
    {
        return newInitializer( securityPlan, Integer.MAX_VALUE );
    }

    private static NettyChannelInitializer newInitializer( SecurityPlan securityPlan, int connectTimeoutMillis )
    {
        return newInitializer( securityPlan, connectTimeoutMillis, new FakeClock() );
    }

    private static NettyChannelInitializer newInitializer( SecurityPlan securityPlan, int connectTimeoutMillis,
            Clock clock )
    {
        return new NettyChannelInitializer( LOCAL_DEFAULT, securityPlan, connectTimeoutMillis, clock,
                DEV_NULL_LOGGING );
    }
}
