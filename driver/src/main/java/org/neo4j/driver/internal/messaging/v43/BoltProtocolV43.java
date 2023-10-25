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
package org.neo4j.driver.internal.messaging.v43;

import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.v42.BoltProtocolV42;

/**
 * Definition of the Bolt Protocol 4.3
 * <p>
 * The version 4.3 use most of the 4.2 behaviours, but it extends it with new messages such as ROUTE
 */
public class BoltProtocolV43 extends BoltProtocolV42 {
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion(4, 3);
    public static final BoltProtocol INSTANCE = new BoltProtocolV43();

    @Override
    public MessageFormat createMessageFormat() {
        return new MessageFormatV43();
    }

    @Override
    public BoltProtocolVersion version() {
        return VERSION;
    }

    @Override
    protected boolean includeDateTimeUtcPatchInHello() {
        return true;
    }
}
