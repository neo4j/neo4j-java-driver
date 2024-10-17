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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.v57;

import org.neo4j.driver.internal.bolt.api.BoltProtocolVersion;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.BoltProtocol;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.v56.BoltProtocolV56;

public class BoltProtocolV57 extends BoltProtocolV56 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(5, 7);
    public static final BoltProtocol INSTANCE = new BoltProtocolV57();

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV57();
    }
}
