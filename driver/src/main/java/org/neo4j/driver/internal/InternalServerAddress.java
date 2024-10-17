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
package org.neo4j.driver.internal;

import java.net.URI;
import java.util.Objects;
import org.neo4j.driver.net.ServerAddress;

public record InternalServerAddress(String host, int port) implements ServerAddress {
    public static final int DEFAULT_PORT = 7687;

    public InternalServerAddress {
        Objects.requireNonNull(host, "host");
        requireValidPort(port);
    }

    private static void requireValidPort(int port) {
        if (port >= 0 && port <= 65_535) {
            return;
        }
        throw new IllegalArgumentException("Illegal port: " + port);
    }

    public InternalServerAddress(String address) {
        this(uriFrom(address));
    }

    public InternalServerAddress(URI uri) {
        this(hostFrom(uri), portFrom(uri));
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

    private static RuntimeException invalidAddressFormat(URI uri) {
        return invalidAddressFormat(uri.toString());
    }

    private static RuntimeException invalidAddressFormat(String address) {
        return new IllegalArgumentException("Invalid address format `" + address + "`");
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

    @Override
    public String toString() {
        return String.format("%s:%d", host, port);
    }
}
