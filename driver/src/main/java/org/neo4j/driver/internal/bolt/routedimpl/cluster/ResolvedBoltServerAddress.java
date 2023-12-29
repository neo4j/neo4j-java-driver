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
package org.neo4j.driver.internal.bolt.routedimpl.cluster;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;

/**
 * An explicitly resolved version of {@link BoltServerAddress} that always contains one or more resolved IP addresses.
 */
class ResolvedBoltServerAddress extends BoltServerAddress {
    private static final String HOST_ADDRESSES_FORMAT = "%s%s:%d";
    private static final int MAX_HOST_ADDRESSES_IN_STRING_VALUE = 5;
    private static final String HOST_ADDRESS_DELIMITER = ",";
    private static final String HOST_ADDRESSES_PREFIX = "(";
    private static final String HOST_ADDRESSES_SUFFIX = ")";
    private static final String TRIMMED_HOST_ADDRESSES_SUFFIX = ",..." + HOST_ADDRESSES_SUFFIX;

    private final Set<InetAddress> resolvedAddresses;
    private final String stringValue;

    public ResolvedBoltServerAddress(String host, int port, InetAddress[] resolvedAddressesArr) {
        super(host, port);
        requireNonNull(resolvedAddressesArr, "resolvedAddressesArr");
        if (resolvedAddressesArr.length == 0) {
            throw new IllegalArgumentException(
                    "The resolvedAddressesArr must not be empty, check your DomainNameResolver is compliant with the interface contract");
        }
        resolvedAddresses = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(resolvedAddressesArr)));
        stringValue = createStringRepresentation();
    }

    /**
     * Create a stream of unicast addresses.
     * <p>
     * The stream is created from the list of resolved IP addresses. Each unicast address is given a unique IP address as the connectionHost value.
     *
     * @return stream of unicast addresses.
     */
    @Override
    public Stream<BoltServerAddress> unicastStream() {
        return resolvedAddresses.stream().map(address -> new BoltServerAddress(host, address.getHostAddress(), port));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        var that = (ResolvedBoltServerAddress) o;
        return resolvedAddresses.equals(that.resolvedAddresses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolvedAddresses);
    }

    @Override
    public String toString() {
        return stringValue;
    }

    private String createStringRepresentation() {
        var hostAddresses = resolvedAddresses.stream()
                .limit(MAX_HOST_ADDRESSES_IN_STRING_VALUE)
                .map(InetAddress::getHostAddress)
                .collect(joining(
                        HOST_ADDRESS_DELIMITER,
                        HOST_ADDRESSES_PREFIX,
                        resolvedAddresses.size() > MAX_HOST_ADDRESSES_IN_STRING_VALUE
                                ? TRIMMED_HOST_ADDRESSES_SUFFIX
                                : HOST_ADDRESSES_SUFFIX));
        return String.format(HOST_ADDRESSES_FORMAT, host, hostAddresses, port);
    }
}
