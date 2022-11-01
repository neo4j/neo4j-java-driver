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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.driver.NotificationFilter;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessageV51;
import org.neo4j.driver.internal.messaging.v5.BoltProtocolV5;
import org.neo4j.driver.internal.security.InternalAuthToken;

public class BoltProtocolV51 extends BoltProtocolV5 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 1);
    public static final BoltProtocol INSTANCE = new BoltProtocolV51();

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    protected HelloMessage createHelloMessage(
            String userAgent,
            Map<String, Value> authToken,
            RoutingContext routingContext,
            boolean includeDateTimeUtc,
            Channel channel) {
        return new HelloMessageV51(
                userAgent,
                ((InternalAuthToken) authToken).toMap(),
                routingContext.toMap(),
                getNotificationFilters(channel));
    }

    @Override
    protected Set<String> mapNotificationFiltersToStrings(Set<NotificationFilter> notificationFilters) {
        if (notificationFilters.size() == 1) {
            var filter = notificationFilters.iterator().next();
            return switch (filter) {
                case NONE -> Collections.emptySet();
                case DEFAULT -> null;
                default -> mapNotificationFiltersToStrings(Set.of(filter));
            };
        }
        return notificationFilters.stream()
                .map(this::mapNotificationFilterToString)
                .collect(Collectors.toSet());
    }

    private String mapNotificationFilterToString(NotificationFilter filter) {
        return switch (filter) {
            case NONE, DEFAULT -> throw new IllegalStateException();
            case ALL_ALL -> "*.*";
            case WARNING_ALL -> "WARNING.*";
            case WARNING_DEPRECATION -> "WARNING.DEPRECATION";
            case WARNING_HINT -> "WARNING.HINT";
            case WARNING_UNRECOGNIZED -> "WARNING.UNRECOGNIZED";
            case WARNING_UNSUPPORTED -> "WARNING.UNSUPPORTED";
            case WARNING_GENERIC -> "WARNING.GENERIC";
            case WARNING_PERFORMANCE -> "WARNING.PERFORMANCE";
            case INFORMATION_ALL -> "INFORMATION.*";
            case INFORMATION_DEPRECATION -> "INFORMATION.DEPRECATION";
            case INFORMATION_HINT -> "INFORMATION.HINT";
            case INFORMATION_UNRECOGNIZED -> "INFORMATION.UNRECOGNIZED";
            case INFORMATION_UNSUPPORTED -> "INFORMATION.UNSUPPORTED";
            case INFORMATION_GENERIC -> "INFORMATION.GENERIC";
            case INFORMATION_PERFORMANCE -> "INFORMATION.PERFORMANCE";
            case ALL_DEPRECATION -> "*.DEPRECATION";
            case ALL_HINT -> "*.HINT";
            case ALL_UNRECOGNIZED -> "*.UNRECOGNIZED";
            case ALL_UNSUPPORTED -> "*.UNSUPPORTED";
            case ALL_GENERIC -> "*.GENERIC";
            case ALL_PERFORMANCE -> "*.PERFORMANCE";
        };
    }

    protected Set<String> getNotificationFilters(Channel channel) {
        var filters = notificationFilters(channel);
        return mapNotificationFiltersToStrings(filters);
    }
}
