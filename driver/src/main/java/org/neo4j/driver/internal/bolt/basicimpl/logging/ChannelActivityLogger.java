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

import static java.lang.String.format;

import io.netty.channel.Channel;
import java.util.ResourceBundle;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes;

public class ChannelActivityLogger implements System.Logger {
    private final Channel channel;
    private final String localChannelId;
    private final System.Logger delegate;

    private String dbConnectionId;
    private String serverAddress;

    public ChannelActivityLogger(Channel channel, LoggingProvider logging, Class<?> owner) {
        this(channel, logging.getLog(owner));
    }

    private ChannelActivityLogger(Channel channel, System.Logger delegate) {
        this.channel = channel;
        this.delegate = delegate;
        this.localChannelId = channel != null ? channel.id().toString() : null;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isLoggable(Level level) {
        return delegate.isLoggable(level);
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String msg, Throwable thrown) {
        delegate.log(level, bundle, reformat(msg), thrown);
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String format, Object... params) {
        delegate.log(level, bundle, reformat(format), params);
    }

    String reformat(String message) {
        if (channel == null) {
            return message;
        }

        var dbConnectionId = getDbConnectionId();
        var serverAddress = getServerAddress();

        return format(
                "[0x%s][%s][%s] %s",
                localChannelId, valueOrEmpty(serverAddress), valueOrEmpty(dbConnectionId), message);
    }

    private String getDbConnectionId() {
        if (dbConnectionId == null) {
            dbConnectionId = ChannelAttributes.connectionId(channel);
        }
        return dbConnectionId;
    }

    private String getServerAddress() {

        if (serverAddress == null) {
            var serverAddress = ChannelAttributes.serverAddress(channel);
            this.serverAddress = serverAddress != null ? serverAddress.toString() : null;
        }

        return serverAddress;
    }

    /**
     * Returns the submitted value if it is not null or an empty string if it is.
     */
    private static String valueOrEmpty(String value) {
        return value != null ? value : "";
    }
}
