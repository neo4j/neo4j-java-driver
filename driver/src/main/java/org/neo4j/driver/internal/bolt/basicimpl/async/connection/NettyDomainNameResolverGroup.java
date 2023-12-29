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
package org.neo4j.driver.internal.bolt.basicimpl.async.connection;

import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;

public class NettyDomainNameResolverGroup extends AddressResolverGroup<InetSocketAddress> {
    private final DomainNameResolver domainNameResolver;

    public NettyDomainNameResolverGroup(DomainNameResolver domainNameResolver) {
        this.domainNameResolver = domainNameResolver;
    }

    @Override
    @SuppressWarnings("resource")
    protected AddressResolver<InetSocketAddress> newResolver(EventExecutor executor) {
        return new NettyDomainNameResolver(executor, domainNameResolver).asAddressResolver();
    }
}
