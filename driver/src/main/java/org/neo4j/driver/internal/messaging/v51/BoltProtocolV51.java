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
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.driver.NotificationFilter;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.v5.BoltProtocolV5;

public class BoltProtocolV51 extends BoltProtocolV5 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 1);
    public static final BoltProtocol INSTANCE = new BoltProtocolV51();

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    @Override
    protected Set<String> getNotificationFilters(Channel channel) {
        var filters = notificationFilters(channel);
        return mapNotificationFiltersToStrings(filters);
    }

    @Override
    protected Set<String> mapNotificationFiltersToStrings(Set<NotificationFilter> notificationFilters) {
        return notificationFilters.stream()
                .map(this::mapNotificationFilterToString)
                .collect(Collectors.toSet());
    }

    private String mapNotificationFilterToString(NotificationFilter filter) {
        return switch (filter) {
            case NONE -> "NONE";
            case ALL_ALL -> "*.*";
            case WARNING_ALL -> "WARNING.*";
            case WARNING_DEPRECATION -> "WARNING.DEPRECATION";
            case WARNING_HINT -> "WARNING.HINT";
            case WARNING_QUERY -> "WARNING.QUERY";
            case WARNING_UNSUPPORTED -> "WARNING.UNSUPPORTED";
            case INFORMATION_ALL -> "INFORMATION.*";
            case INFORMATION_RUNTIME -> "INFORMATION.RUNTIME";
            case INFORMATION_QUERY -> "INFORMATION.QUERY";
            case INFORMATION_PERFORMANCE -> "INFORMATION.PERFORMANCE";
            case ALL_QUERY -> "*.QUERY";
        };
    }
}
