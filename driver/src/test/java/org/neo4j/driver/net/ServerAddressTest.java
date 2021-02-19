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
package org.neo4j.driver.net;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerAddressTest
{
    @Test
    void shouldCreateAddress()
    {
        ServerAddress address = ServerAddress.of( "my.database.com", 8897 );
        assertEquals( "my.database.com", address.host() );
        assertEquals( 8897, address.port() );
    }

    @Test
    void shouldFailToCreateAddressWithInvalidHost()
    {
        assertThrows( NullPointerException.class, () -> ServerAddress.of( null, 9999 ) );
    }

    @Test
    void shouldFailToCreateAddressWithInvalidPort()
    {
        assertThrows( IllegalArgumentException.class, () -> ServerAddress.of( "hello.graphs.com", -42 ) );
        assertThrows( IllegalArgumentException.class, () -> ServerAddress.of( "hello.graphs.com", 66_000 ) );
    }
}
