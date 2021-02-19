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
package org.neo4j.driver.internal.spi;

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.DatabaseName;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.util.ServerVersion;

import static java.lang.String.format;

public interface Connection
{
    boolean isOpen();

    void enableAutoRead();

    void disableAutoRead();

    void write( Message message, ResponseHandler handler );

    void write( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 );

    void writeAndFlush( Message message, ResponseHandler handler );

    void writeAndFlush( Message message1, ResponseHandler handler1, Message message2, ResponseHandler handler2 );

    CompletionStage<Void> reset();

    CompletionStage<Void> release();

    void terminateAndRelease( String reason );

    BoltServerAddress serverAddress();

    ServerVersion serverVersion();

    BoltProtocol protocol();

    default AccessMode mode()
    {
        throw new UnsupportedOperationException( format( "%s does not support access mode.", getClass() ) );
    }

    default DatabaseName databaseName()
    {
        throw new UnsupportedOperationException( format( "%s does not support database name.", getClass() ) );
    }

    void flush();
}
