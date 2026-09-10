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
package org.neo4j.driver.internal.observation;

import java.net.URI;
import java.util.List;
import java.util.function.BiConsumer;
import org.neo4j.bolt.connection.BoltProtocolVersion;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.BaseSession;
import org.neo4j.driver.property_encryption.BasePropertyEncryption;
import org.neo4j.driver.types.MapAccessor;

public class NoopObservationProvider implements DriverObservationProvider {
    private static final NoopObservationProvider INSTANCE = new NoopObservationProvider();

    public static NoopObservationProvider getInstance() {
        return INSTANCE;
    }

    public NoopObservationProvider() {}

    @Override
    public Observation sessionRun(Class<? extends BaseSession> sessionType, String query, MapAccessor parameters) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation beginTransaction(Class<?> transactionType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation sessionExecute(Class<? extends BaseSession> sessionType, AccessMode mode) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation sessionClose(Class<? extends BaseSession> sessionType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation transactionRun(Class<?> transactionType, String query, MapAccessor parameters) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation transactionCommit(Class<?> transactionType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation transactionRollback(Class<?> transactionType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation transactionClose(Class<?> transactionType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation resultPeek(Class<?> resultType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation resultNext(Class<?> resultType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation resultSingle(Class<?> resultType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation resultList(Class<?> resultType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation resultConsume(Class<?> resultType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation resultRecords(Class<?> resultType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation encryptToBytes(Class<? extends BasePropertyEncryption> propertyEncryptionType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation decrypt(Class<? extends BasePropertyEncryption> propertyEncryptionType) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation createEncapsulatedKey(Class<?> encapsulatedKeyManagerType, String alias) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation findEncapsulatedKeyByAlias(Class<?> encapsulatedKeyManagerType, String alias) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation setEncapsulatedKeyAlias(Class<?> encapsulatedKeyManagerType, String id, String alias) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation deleteEncapsulatedKey(Class<?> encapsulatedKeyManagerType, String id) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation keyEncapsulationServiceEncapsulate() {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation keyEncapsulationServiceDecapsulate() {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation encapsulatedKeyRepositoryFindById() {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation encapsulatedKeyRepositoryFindByAlias() {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation encapsulatedKeyRepositoryCreate() {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation encapsulatedKeyRepositorySetAliasById() {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation encapsulatedKeyRepositoryDeleteById() {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation connectionPoolCreate(String id, URI uri, int maxSize) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation connectionPoolClose(String id, URI uri) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation pooledConnectionCreate(String id, URI uri) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation pooledConnectionClose(String id, URI uri) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation pooledConnectionAcquire(String id, URI uri) {
        return NoopObservation.getInstance();
    }

    @Override
    public Observation pooledConnectionInUse(String id, URI uri) {
        return NoopObservation.getInstance();
    }

    @Override
    public BoltHandleObservation boltHandle(List<String> messageTypes) {
        return NoopBoltHandleObservation.getInstance();
    }

    @Override
    public BoltExchangeObservation boltExchange(
            String host, int port, BoltProtocolVersion boltVersion, BiConsumer<String, String> setter) {
        return NoopBoltExchangeObservation.getInstance();
    }

    @Override
    public HttpExchangeObservation httpExchange(
            URI uri, String method, String uriTemplate, BiConsumer<String, String> setter) {
        return NoopHttpExchangeObservation.getInstance();
    }

    @Override
    public Observation scopedObservation() {
        return null;
    }
}
