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
package org.neo4j.driver.internal.messaging.encode;

import static org.neo4j.driver.Values.value;
import static org.neo4j.driver.internal.util.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageEncoder;
import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.messaging.request.RouteMessage;

/**
 * Encodes the ROUTE message to the stream
 */
public class RouteV44MessageEncoder implements MessageEncoder {
    @Override
    public void encode(Message message, ValuePacker packer) throws IOException {
        checkArgument(message, RouteMessage.class);
        RouteMessage routeMessage = (RouteMessage) message;
        packer.packStructHeader(3, message.signature());
        packer.pack(routeMessage.getRoutingContext());
        packer.pack(
                routeMessage.getBookmark().isPresent()
                        ? value(routeMessage.getBookmark().get().values())
                        : value(Collections.emptyList()));

        Map<String, Value> params;
        if (routeMessage.getImpersonatedUser() != null && routeMessage.getDatabaseName() == null) {
            params = Collections.singletonMap("imp_user", value(routeMessage.getImpersonatedUser()));
        } else if (routeMessage.getDatabaseName() != null) {
            params = Collections.singletonMap("db", value(routeMessage.getDatabaseName()));
        } else {
            params = Collections.emptyMap();
        }
        packer.pack(params);
    }
}
