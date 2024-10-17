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
package org.neo4j.driver.internal.bolt.basicimpl;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import java.time.Clock;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;
import org.neo4j.driver.internal.bolt.api.LoggingProvider;

public class ConnectionProviders {
    static ConnectionProvider netty(
            EventLoopGroup group,
            Clock clock,
            DomainNameResolver domainNameResolver,
            LocalAddress localAddress,
            LoggingProvider logging) {
        return new NettyConnectionProvider(group, clock, domainNameResolver, localAddress, logging);
    }
}
