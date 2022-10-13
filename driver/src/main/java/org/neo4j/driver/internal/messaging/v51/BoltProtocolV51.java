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
package org.neo4j.driver.internal.messaging.v51;

import static org.neo4j.driver.internal.async.connection.ChannelAttributes.notificationFilters;

import io.netty.channel.Channel;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.encode.NotificationFilterEncodingUtil;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessageV51;
import org.neo4j.driver.internal.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.summary.InternalNotificationFilterConfig;
import org.neo4j.driver.summary.NotificationFilterConfig;

public class BoltProtocolV51 extends BoltProtocolV5 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 1);
    public static final BoltProtocol INSTANCE = new BoltProtocolV51();

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV51();
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    protected HelloMessage createHelloMessage(
            String userAgent,
            Map<String, Value> authToken,
            Map<String, String> routingContext,
            boolean includeDateTimeUtc,
            Channel channel) {
        var notificationFilters = NotificationFilterEncodingUtil.encode(notificationFilters(channel), false);
        return new HelloMessageV51(userAgent, authToken, routingContext, notificationFilters);
    }

    @Override
    protected Value toNotificationFiltersValue(NotificationFilterConfig notificationFilterConfig) {
        return notificationFilterConfig == null
                ? null
                : NotificationFilterEncodingUtil.encode(
                        ((InternalNotificationFilterConfig) notificationFilterConfig).filters(), true);
    }
}
