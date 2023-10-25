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
package org.neo4j.driver.internal.net;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.BoltServerAddress.DEFAULT_PORT;

import java.net.URI;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.net.ServerAddress;

class BoltServerAddressTest {
    @Test
    void defaultPortShouldBe7687() {
        assertThat(DEFAULT_PORT, equalTo(7687));
    }

    @Test
    void portShouldUseDefaultIfNotSupplied() {
        assertThat(new BoltServerAddress("localhost").port(), equalTo(BoltServerAddress.DEFAULT_PORT));
    }

    @Test
    void shouldHaveCorrectToString() {
        assertEquals("localhost:4242", new BoltServerAddress("localhost", 4242).toString());
        assertEquals("127.0.0.1:8888", new BoltServerAddress("127.0.0.1", 8888).toString());
    }

    @Test
    void shouldVerifyHost() {
        assertThrows(NullPointerException.class, () -> new BoltServerAddress(null, 0));
    }

    @Test
    void shouldVerifyPort() {
        assertThrows(IllegalArgumentException.class, () -> new BoltServerAddress("localhost", -1));
        assertThrows(IllegalArgumentException.class, () -> new BoltServerAddress("localhost", -42));
        assertThrows(IllegalArgumentException.class, () -> new BoltServerAddress("localhost", 65_536));
        assertThrows(IllegalArgumentException.class, () -> new BoltServerAddress("localhost", 99_999));
    }

    @Test
    void shouldCreateBoltServerAddressFromServerAddress() {
        var address1 = new BoltServerAddress("my.server.com", 8899);
        assertSame(address1, BoltServerAddress.from(address1));

        var address2 = new BoltServerAddress("db.neo4j.com");
        assertSame(address2, BoltServerAddress.from(address2));

        var address3 = mock(ServerAddress.class);
        when(address3.host()).thenReturn("graph.database.com");
        when(address3.port()).thenReturn(20600);
        assertEquals(new BoltServerAddress("graph.database.com", 20600), BoltServerAddress.from(address3));
    }

    @Test
    void shouldFailToCreateBoltServerAddressFromInvalidServerAddress() {
        var address1 = mock(ServerAddress.class);
        when(address1.host()).thenReturn(null);
        when(address1.port()).thenReturn(8888);
        assertThrows(NullPointerException.class, () -> BoltServerAddress.from(address1));

        var address2 = mock(ServerAddress.class);
        when(address2.host()).thenReturn("neo4j.host.com");
        when(address2.port()).thenReturn(-1);
        assertThrows(IllegalArgumentException.class, () -> BoltServerAddress.from(address2));

        var address3 = mock(ServerAddress.class);
        when(address3.host()).thenReturn("my.database.org");
        when(address3.port()).thenReturn(99_000);
        assertThrows(IllegalArgumentException.class, () -> BoltServerAddress.from(address3));
    }

    @Test
    void shouldUseUriWithHostButWithoutPort() {
        var uri = URI.create("bolt://neo4j.com");
        var address = new BoltServerAddress(uri);

        assertEquals("neo4j.com", address.host());
        assertEquals(DEFAULT_PORT, address.port());
    }

    @Test
    void shouldUseUriWithHostAndPort() {
        var uri = URI.create("bolt://neo4j.com:12345");
        var address = new BoltServerAddress(uri);

        assertEquals("neo4j.com", address.host());
        assertEquals(12345, address.port());
    }

    @Test
    void shouldIncludeHostAndPortInToString() {
        var address = new BoltServerAddress("localhost", 8081);
        assertThat(address.toString(), equalTo("localhost:8081"));
    }
}
