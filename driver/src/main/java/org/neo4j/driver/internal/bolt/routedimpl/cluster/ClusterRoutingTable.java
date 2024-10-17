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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.neo4j.driver.internal.bolt.routedimpl.util.LockUtil.executeWithLock;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.driver.internal.bolt.api.AccessMode;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.ClusterComposition;
import org.neo4j.driver.internal.bolt.api.DatabaseName;

public class ClusterRoutingTable implements RoutingTable {
    private final ReadWriteLock tableLock = new ReentrantReadWriteLock();
    private final DatabaseName databaseName;
    private final Clock clock;
    private final Set<BoltServerAddress> disused = new HashSet<>();

    private long expirationTimestamp;
    private boolean preferInitialRouter = true;
    private List<BoltServerAddress> readers = Collections.emptyList();
    private List<BoltServerAddress> writers = Collections.emptyList();
    private List<BoltServerAddress> routers = Collections.emptyList();

    public ClusterRoutingTable(DatabaseName ofDatabase, Clock clock, BoltServerAddress... routingAddresses) {
        this(ofDatabase, clock);
        routers = Collections.unmodifiableList(asList(routingAddresses));
    }

    private ClusterRoutingTable(DatabaseName ofDatabase, Clock clock) {
        this.databaseName = ofDatabase;
        this.clock = clock;
        this.expirationTimestamp = clock.millis() - 1;
    }

    @Override
    public boolean isStaleFor(AccessMode mode) {
        return executeWithLock(
                tableLock.readLock(),
                () -> expirationTimestamp < clock.millis()
                        || routers.isEmpty()
                        || mode == AccessMode.READ && readers.isEmpty()
                        || mode == AccessMode.WRITE && writers.isEmpty());
    }

    @Override
    public boolean hasBeenStaleFor(long extraTime) {
        var totalTime = executeWithLock(tableLock.readLock(), () -> expirationTimestamp) + extraTime;
        if (totalTime < 0) {
            totalTime = Long.MAX_VALUE;
        }
        return totalTime < clock.millis();
    }

    @Override
    public void update(ClusterComposition cluster) {
        executeWithLock(tableLock.writeLock(), () -> {
            expirationTimestamp = cluster.expirationTimestamp();
            readers = newWithReusedAddresses(readers, disused, cluster.readers());
            writers = newWithReusedAddresses(writers, disused, cluster.writers());
            routers = newWithReusedAddresses(routers, disused, cluster.routers());
            disused.clear();
            preferInitialRouter = !cluster.hasWriters();
        });
    }

    @Override
    public void forget(BoltServerAddress address) {
        executeWithLock(tableLock.writeLock(), () -> {
            routers = newWithoutAddressIfPresent(routers, address);
            readers = newWithoutAddressIfPresent(readers, address);
            writers = newWithoutAddressIfPresent(writers, address);
            disused.add(address);
        });
    }

    @Override
    public List<BoltServerAddress> readers() {
        return executeWithLock(tableLock.readLock(), () -> readers);
    }

    @Override
    public List<BoltServerAddress> writers() {
        return executeWithLock(tableLock.readLock(), () -> writers);
    }

    @Override
    public List<BoltServerAddress> routers() {
        return executeWithLock(tableLock.readLock(), () -> routers);
    }

    @Override
    public Set<BoltServerAddress> servers() {
        return executeWithLock(tableLock.readLock(), () -> {
            Set<BoltServerAddress> servers = new HashSet<>();
            servers.addAll(readers);
            servers.addAll(writers);
            servers.addAll(routers);
            servers.addAll(disused);
            return servers;
        });
    }

    @Override
    public DatabaseName database() {
        return databaseName;
    }

    @Override
    public void forgetWriter(BoltServerAddress toRemove) {
        executeWithLock(tableLock.writeLock(), () -> {
            writers = newWithoutAddressIfPresent(writers, toRemove);
            disused.add(toRemove);
        });
    }

    @Override
    public void replaceRouterIfPresent(BoltServerAddress oldRouter, BoltServerAddress newRouter) {
        executeWithLock(
                tableLock.writeLock(), () -> routers = newWithAddressReplacedIfPresent(routers, oldRouter, newRouter));
    }

    @Override
    public boolean preferInitialRouter() {
        return executeWithLock(tableLock.readLock(), () -> preferInitialRouter);
    }

    @Override
    public long expirationTimestamp() {
        return executeWithLock(tableLock.readLock(), () -> expirationTimestamp);
    }

    @Override
    public String toString() {
        return executeWithLock(
                tableLock.readLock(),
                () -> format(
                        "Ttl %s, currentTime %s, routers %s, writers %s, readers %s, database '%s'",
                        expirationTimestamp, clock.millis(), routers, writers, readers, databaseName.description()));
    }

    private List<BoltServerAddress> newWithoutAddressIfPresent(
            List<BoltServerAddress> addresses, BoltServerAddress addressToSkip) {
        return addresses.stream()
                .filter(address -> !address.equals(addressToSkip))
                .toList();
    }

    private List<BoltServerAddress> newWithAddressReplacedIfPresent(
            List<BoltServerAddress> addresses, BoltServerAddress oldAddress, BoltServerAddress newAddress) {
        return addresses.stream()
                .map(address -> address.equals(oldAddress) ? newAddress : address)
                .toList();
    }

    private List<BoltServerAddress> newWithReusedAddresses(
            List<BoltServerAddress> currentAddresses,
            Set<BoltServerAddress> disusedAddresses,
            Set<BoltServerAddress> newAddresses) {
        List<BoltServerAddress> newList = Stream.concat(currentAddresses.stream(), disusedAddresses.stream())
                .filter(address -> newAddresses.remove(toBoltServerAddress(address)))
                .collect(Collectors.toCollection(() -> new ArrayList<>(newAddresses.size())));
        newList.addAll(newAddresses);
        return Collections.unmodifiableList(newList);
    }

    private BoltServerAddress toBoltServerAddress(BoltServerAddress address) {
        return BoltServerAddress.class.equals(address.getClass())
                ? address
                : new BoltServerAddress(address.host(), address.port());
    }
}
