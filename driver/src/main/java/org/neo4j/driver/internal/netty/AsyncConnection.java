/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.netty;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.Map;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.v1.Value;

public interface AsyncConnection
{
    void run( String statement, Map<String,Value> parameters, ResponseHandler handler );

    void pullAll( ResponseHandler handler );

    void discardAll( ResponseHandler handler );

    void reset( ResponseHandler handler );

    void resetAsync( ResponseHandler handler );

    void flush();

    // todo: create promise who's callbacks are executed in this channel's event loop
    // todo: do we need this???
    <T> Promise<T> newPromise();

    // todo: run a command in this channel's event loop
    // todo: do we need this???
    void execute( Runnable command );

    Future<Channel> channelFuture();

    boolean isOpen();

    void release();

    BoltServerAddress boltServerAddress();
}
