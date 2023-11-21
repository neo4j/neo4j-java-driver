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
package org.neo4j.driver.internal.async.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.internal.DatabaseNameUtil.defaultDatabase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ResponseHandler;
import org.neo4j.driver.net.ServerAddress;

class DecoratedConnectionTest {
    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void shouldDelegateIsOpen(String open) {
        var mockConnection = mock(Connection.class);
        when(mockConnection.isOpen()).thenReturn(Boolean.valueOf(open));

        var connection = newConnection(mockConnection);

        assertEquals(Boolean.valueOf(open), connection.isOpen());
        verify(mockConnection).isOpen();
    }

    @Test
    void shouldDelegateEnableAutoRead() {
        var mockConnection = mock(Connection.class);
        var connection = newConnection(mockConnection);

        connection.enableAutoRead();

        verify(mockConnection).enableAutoRead();
    }

    @Test
    void shouldDelegateDisableAutoRead() {
        var mockConnection = mock(Connection.class);
        var connection = newConnection(mockConnection);

        connection.disableAutoRead();

        verify(mockConnection).disableAutoRead();
    }

    @Test
    void shouldDelegateWrite() {
        var mockConnection = mock(Connection.class);
        var connection = newConnection(mockConnection);

        var message = mock(Message.class);
        var handler = mock(ResponseHandler.class);

        connection.write(message, handler);

        verify(mockConnection).write(message, handler);
    }

    @Test
    void shouldDelegateWriteAndFlush() {
        var mockConnection = mock(Connection.class);
        var connection = newConnection(mockConnection);

        var message = mock(Message.class);
        var handler = mock(ResponseHandler.class);

        connection.writeAndFlush(message, handler);

        verify(mockConnection).writeAndFlush(message, handler);
    }

    @Test
    void shouldDelegateReset() {
        var mockConnection = mock(Connection.class);
        var connection = newConnection(mockConnection);

        connection.reset(null);

        verify(mockConnection).reset(null);
    }

    @Test
    void shouldDelegateRelease() {
        var mockConnection = mock(Connection.class);
        var connection = newConnection(mockConnection);

        connection.release();

        verify(mockConnection).release();
    }

    @Test
    void shouldDelegateTerminateAndRelease() {
        var mockConnection = mock(Connection.class);
        var connection = newConnection(mockConnection);

        connection.terminateAndRelease("a reason");

        verify(mockConnection).terminateAndRelease("a reason");
    }

    @Test
    void shouldDelegateServerAddress() {
        var address = BoltServerAddress.from(ServerAddress.of("localhost", 9999));
        var mockConnection = mock(Connection.class);
        when(mockConnection.serverAddress()).thenReturn(address);
        var connection = newConnection(mockConnection);

        assertSame(address, connection.serverAddress());
        verify(mockConnection).serverAddress();
    }

    @Test
    void shouldDelegateProtocol() {
        var protocol = mock(BoltProtocol.class);
        var mockConnection = mock(Connection.class);
        when(mockConnection.protocol()).thenReturn(protocol);
        var connection = newConnection(mockConnection);

        assertSame(protocol, connection.protocol());
        verify(mockConnection).protocol();
    }

    @ParameterizedTest
    @EnumSource(AccessMode.class)
    void shouldReturnModeFromConstructor(AccessMode mode) {
        var connection = new DirectConnection(mock(Connection.class), defaultDatabase(), mode, null);

        assertEquals(mode, connection.mode());
    }

    @Test
    void shouldReturnConnection() {
        var mockConnection = mock(Connection.class);
        var connection = newConnection(mockConnection);

        assertSame(mockConnection, connection.connection());
    }

    private static DirectConnection newConnection(Connection connection) {
        return new DirectConnection(connection, defaultDatabase(), READ, null);
    }
}
