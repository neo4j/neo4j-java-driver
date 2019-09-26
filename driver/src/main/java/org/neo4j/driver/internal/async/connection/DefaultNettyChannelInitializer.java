/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.internal.async.connection;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.Logging;

public class DefaultNettyChannelInitializer extends AbstractNettyChannelInitializer
{
    private final BoltServerAddress address;
    private final SecurityPlan securityPlan;
    private final int connectTimeoutMillis;

    public DefaultNettyChannelInitializer( BoltServerAddress address, SecurityPlan securityPlan, int connectTimeoutMillis, Clock clock, Logging logging )
    {
        super( address, clock, logging );
        this.address = address;
        this.securityPlan = securityPlan;
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    @Override
    protected void addSslHandler( Channel channel )
    {
        if ( securityPlan.requiresEncryption() )
        {
            SslHandler sslHandler = createSslHandler();
            channel.pipeline().addFirst( sslHandler );
        }
    }

    private SslHandler createSslHandler()
    {
        SSLEngine sslEngine = createSslEngine();
        SslHandler sslHandler = new SslHandler( sslEngine );
        sslHandler.setHandshakeTimeoutMillis( connectTimeoutMillis );
        return sslHandler;
    }

    private SSLEngine createSslEngine()
    {
        SSLContext sslContext = securityPlan.sslContext();
        SSLEngine sslEngine = sslContext.createSSLEngine( address.host(), address.port() );
        sslEngine.setUseClientMode( true );
        if ( securityPlan.requiresHostnameVerification() )
        {
            SSLParameters sslParameters = sslEngine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm( "HTTPS" );
            sslEngine.setSSLParameters( sslParameters );
        }
        return sslEngine;
    }
}
