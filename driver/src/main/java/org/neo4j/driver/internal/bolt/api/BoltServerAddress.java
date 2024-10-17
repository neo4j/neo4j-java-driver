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
package org.neo4j.driver.internal.bolt.api;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Holds a host and port pair that denotes a Bolt server address.
 */
public class BoltServerAddress {
    public static final int DEFAULT_PORT = 7687;
    public static final BoltServerAddress LOCAL_DEFAULT = new BoltServerAddress("localhost", DEFAULT_PORT);

    protected final String host; // Host or IP address.
    private final String
            connectionHost; // Either is equal to the host or is explicitly provided on creation and is expected to be a
    // resolved IP address.
    protected final int port;
    private final String stringValue;

    public BoltServerAddress(String address) {
        this(uriFrom(address));
    }

    public BoltServerAddress(URI uri) {
        this(hostFrom(uri), portFrom(uri));
    }

    public BoltServerAddress(String host, int port) {
        this(host, host, port);
    }

    public BoltServerAddress(String host, String connectionHost, int port) {
        this.host = requireNonNull(host, "host");
        this.connectionHost = requireNonNull(connectionHost, "connectionHost");
        this.port = requireValidPort(port);
        this.stringValue = host.equals(connectionHost)
                ? String.format("%s:%d", host, port)
                : String.format("%s(%s):%d", host, connectionHost, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        var address = (BoltServerAddress) o;
        return port == address.port && host.equals(address.host) && connectionHost.equals(address.connectionHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, connectionHost, port);
    }

    @Override
    public String toString() {
        return stringValue;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String connectionHost() {
        return connectionHost;
    }

    /**
     * Create a stream of unicast addresses.
     * <p>
     * While this implementation just returns a stream of itself, the subclasses may provide multiple addresses.
     *
     * @return stream of unicast addresses.
     */
    public Stream<BoltServerAddress> unicastStream() {
        return Stream.of(this);
    }

    private static String hostFrom(URI uri) {
        var host = uri.getHost();
        if (host == null) {
            throw invalidAddressFormat(uri);
        }
        return host;
    }

    private static int portFrom(URI uri) {
        var port = uri.getPort();
        return port == -1 ? DEFAULT_PORT : port;
    }

    @SuppressWarnings("DuplicatedCode")
    private static URI uriFrom(String address) {
        String scheme;
        String hostPort;

        var schemeSplit = address.split("://");
        if (schemeSplit.length == 1) {
            // URI can't parse addresses without scheme, prepend fake "bolt://" to reuse the parsing facility
            scheme = "bolt://";
            hostPort = hostPortFrom(schemeSplit[0]);
        } else if (schemeSplit.length == 2) {
            scheme = schemeSplit[0] + "://";
            hostPort = hostPortFrom(schemeSplit[1]);
        } else {
            throw invalidAddressFormat(address);
        }

        return URI.create(scheme + hostPort);
    }

    private static String hostPortFrom(String address) {
        if (address.startsWith("[")) {
            // expected to be an IPv6 address like [::1] or [::1]:7687
            return address;
        }

        var containsSingleColon = address.indexOf(":") == address.lastIndexOf(":");
        if (containsSingleColon) {
            // expected to be an IPv4 address with or without port like 127.0.0.1 or 127.0.0.1:7687
            return address;
        }

        // address contains multiple colons and does not start with '['
        // expected to be an IPv6 address without brackets
        return "[" + address + "]";
    }

    private static RuntimeException invalidAddressFormat(URI uri) {
        return invalidAddressFormat(uri.toString());
    }

    private static RuntimeException invalidAddressFormat(String address) {
        return new IllegalArgumentException("Invalid address format `" + address + "`");
    }

    private static int requireValidPort(int port) {
        if (port >= 0 && port <= 65_535) {
            return port;
        }
        throw new IllegalArgumentException("Illegal port: " + port);
    }
}
