/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.connector.socket;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.security.GeneralSecurityException;

import org.neo4j.driver.internal.spi.Logger;

/**
 * SSLSocketChannel + size-changeable buffer
 */
public class SSLTestSocketChannel extends SSLSocketChannel
{

    public SSLTestSocketChannel( String host, int port, SocketChannel channel, Logger logger,
            int appBufferSize, int netBufferSize )
            throws GeneralSecurityException, IOException
    {
        super( host, port, channel, logger, appBufferSize, netBufferSize );
    }

    public void setBufferSize( int appBufferSize, int netBufferSize )
    {
        createBuffers( appBufferSize, netBufferSize );
    }
}
