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
package org.neo4j.driver.internal.spi;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.async.BoltServerAddress;
import org.neo4j.driver.internal.util.ServerVersion;
import org.neo4j.driver.v1.Value;

public interface Connection
{
    boolean isInUse();

    boolean tryMarkInUse();

    void enableAutoRead();

    void disableAutoRead();

    void run( String statement, Map<String,Value> parameters, ResponseHandler runHandler,
            ResponseHandler pullAllHandler );

    void runAndFlush( String statement, Map<String,Value> parameters, ResponseHandler runHandler,
            ResponseHandler pullAllHandler );

    void release();

    CompletionStage<Void> forceRelease();

    BoltServerAddress serverAddress();

    ServerVersion serverVersion();
}