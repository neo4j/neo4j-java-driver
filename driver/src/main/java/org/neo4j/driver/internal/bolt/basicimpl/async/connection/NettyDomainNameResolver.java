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

import io.netty.resolver.InetNameResolver;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import org.neo4j.driver.internal.bolt.api.DomainNameResolver;

public class NettyDomainNameResolver extends InetNameResolver {
    private final DomainNameResolver domainNameResolver;

    public NettyDomainNameResolver(EventExecutor executor, DomainNameResolver domainNameResolver) {
        super(executor);
        this.domainNameResolver = domainNameResolver;
    }

    @Override
    protected void doResolve(String inetHost, Promise<InetAddress> promise) {
        try {
            promise.setSuccess(domainNameResolver.resolve(inetHost)[0]);
        } catch (UnknownHostException e) {
            promise.setFailure(e);
        }
    }

    @Override
    protected void doResolveAll(String inetHost, Promise<List<InetAddress>> promise) {
        try {
            promise.setSuccess(Arrays.asList(domainNameResolver.resolve(inetHost)));
        } catch (UnknownHostException e) {
            promise.setFailure(e);
        }
    }
}
