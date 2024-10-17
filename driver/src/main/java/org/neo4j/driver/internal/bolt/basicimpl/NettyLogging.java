/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.bolt.basicimpl;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;

/**
 * This is the logging factory to delegate netty's logging to our logging system
 */
public class NettyLogging extends InternalLoggerFactory {
    private final LoggingProvider logging;

    public NettyLogging(LoggingProvider logging) {
        this.logging = logging;
    }

    @Override
    protected InternalLogger newInstance(String name) {
        return new NettyLogger(name, logging.getLog(name));
    }
}
