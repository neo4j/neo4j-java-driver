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
package org.neo4j.driver.internal.cluster;

import org.junit.jupiter.api.Test;

import java.util.Set;

import org.neo4j.driver.net.ServerAddress;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.cluster.IdentityResolver.IDENTITY_RESOLVER;

class IdentityResolverTest
{
    @Test
    void shouldReturnGivenAddress()
    {
        ServerAddress address = mock( ServerAddress.class );

        Set<ServerAddress> resolved = IDENTITY_RESOLVER.resolve( address );

        assertThat( resolved.size(), equalTo( 1 ) );
        assertThat( resolved.iterator().next(), is( address ) );
    }
}
