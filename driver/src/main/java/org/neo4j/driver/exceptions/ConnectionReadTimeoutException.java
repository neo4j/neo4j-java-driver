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
package org.neo4j.driver.exceptions;

/**
 * Indicates that read timed out due to it taking longer than the server-supplied timeout value via the {@code connection.recv_timeout_seconds} configuration
 * hint. The server might provide this value to clients to let them know when a given connection may be considered broken if client does not get any
 * communication from the server within the specified timeout period. This results in the server being removed from the routing table.
 */
public class ConnectionReadTimeoutException extends ServiceUnavailableException {
    public static final ConnectionReadTimeoutException INSTANCE = new ConnectionReadTimeoutException(
            "Connection read timed out due to it taking longer than the server-supplied timeout value via configuration hint.");

    public ConnectionReadTimeoutException(String message) {
        super(message);
    }
}
