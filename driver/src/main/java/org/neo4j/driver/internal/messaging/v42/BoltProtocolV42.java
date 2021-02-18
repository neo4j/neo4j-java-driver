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
package org.neo4j.driver.internal.messaging.v42;

import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.BoltProtocolVersion;
import org.neo4j.driver.internal.messaging.v41.BoltProtocolV41;

/**
 * Bolt V4.2 is identical to V4.1
 */
public class BoltProtocolV42 extends BoltProtocolV41
{
    public static final BoltProtocolVersion VERSION = new BoltProtocolVersion( 4, 2 );
    public static final BoltProtocol INSTANCE = new BoltProtocolV42();

    @Override
    public BoltProtocolVersion version()
    {
        return VERSION;
    }
}
