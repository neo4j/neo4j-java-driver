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
package org.neo4j.driver.internal.bolt.basicimpl.logging;

import io.netty.channel.Channel;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;

/**
 * Dedicated logger for channel error logging.
 * <p>
 * It keeps messages shorter in debug mode and provides more details in trace mode.
 */
public class ChannelErrorLogger extends ChannelActivityLogger {
    private static final String DEBUG_MESSAGE_FORMAT = "%s (%s)";

    public ChannelErrorLogger(Channel channel, LoggingProvider logging) {
        super(channel, logging, ChannelErrorLogger.class);
    }

    public void traceOrDebug(String message, Throwable error) {
        if (isLoggable(Level.TRACE)) {
            log(Level.ERROR, message, error);
        } else {
            log(Level.DEBUG, String.format(DEBUG_MESSAGE_FORMAT, message, error.getClass()));
        }
    }
}
