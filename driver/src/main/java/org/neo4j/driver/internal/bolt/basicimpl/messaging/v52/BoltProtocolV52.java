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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v52;

import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.api.NotificationConfig;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v51.BoltProtocolV51;

public class BoltProtocolV52 extends BoltProtocolV51 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 2);
    public static final BoltProtocol INSTANCE = new BoltProtocolV52();

    @Override
    protected Neo4jException verifyNotificationConfigSupported(NotificationConfig notificationConfig) {
        return null;
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }
}
